use std::num::{NonZeroU128, NonZeroU64, ParseIntError};
use std::str::FromStr;
use std::sync::Arc;

use http::HeaderMap;
use itertools::Itertools;
use snafu::Snafu;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::MetadataMap;
use trace::{SpanContext, SpanId, TraceExporter, TraceId};

pub const DEFAULT_TRACE_HEADER_NAME: &str = "uber-trace-id";

/// Error decoding SpanContext from transport representation
#[derive(Debug, Snafu)]
pub enum ContextError {
    #[snafu(display("header '{}' not found", header))]
    Missing { header: String },

    #[snafu(display("header '{}' has non-UTF8 content: {}", header, source))]
    InvalidUtf8 {
        header: String,
        source: http::header::ToStrError,
    },

    #[snafu(display("decoding header '{}': {}", header, source))]
    HeaderDecodeError { header: String, source: DecodeError },
}

/// Error decoding a specific header value
#[derive(Debug, Snafu)]
pub enum DecodeError {
    #[snafu(display("value decode error: {}", source))]
    ValueDecodeError { source: ParseIntError },

    #[snafu(display("Expected \"trace-id:span-id:parent-span-id:flags\", found: {}", value))]
    InvalidJaegerTrace { value: String },

    #[snafu(display("value cannot be 0"))]
    ZeroError,
}

impl From<ParseIntError> for DecodeError {
    // Snafu doesn't allow both no context and a custom message
    fn from(source: ParseIntError) -> Self {
        Self::ValueDecodeError { source }
    }
}

fn parse_trace(s: &str) -> Result<TraceId, DecodeError> {
    Ok(TraceId(
        NonZeroU128::new(u128::from_str_radix(s, 16)?).ok_or(DecodeError::ZeroError)?,
    ))
}

fn parse_span(s: &str) -> Result<SpanId, DecodeError> {
    Ok(SpanId(
        NonZeroU64::new(u64::from_str_radix(s, 16)?).ok_or(DecodeError::ZeroError)?,
    ))
}

#[derive(Debug, Clone)]
pub struct SpanContextExtractor {
    trace_header_parser: TraceHeaderParser,
    collector: Option<Arc<dyn TraceExporter>>,
}

impl SpanContextExtractor {
    pub fn new(
        trace_header_parser: TraceHeaderParser,
        collector: Option<Arc<dyn TraceExporter>>,
    ) -> Self {
        Self {
            trace_header_parser,
            collector,
        }
    }

    pub fn extract_from_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<SpanContext>, ContextError> {
        if self.collector.is_none() {
            return Ok(None);
        }

        self.trace_header_parser
            .parse(self.collector.clone(), headers)
    }

    pub fn extract_from_value(
        &self,
        trace_header: &str,
        value: Option<String>,
    ) -> Result<Option<SpanContext>, ContextError> {
        if self.collector.is_none() {
            return Ok(None);
        }

        self.trace_header_parser
            .parse_str(self.collector.clone(), trace_header, value)
    }
}

/// Extracts tracing information such as the `SpanContext`s , if any,
/// from http request headers.
#[derive(Debug, Clone, Default)]
pub struct TraceHeaderParser {
    /// header that contains pre-existing trace context, if any
    jaeger_trace_context_header_name: Option<Arc<str>>,
    auto_generate_span: bool,
}

impl TraceHeaderParser {
    /// Create a new span context parser with default Jaeger trace
    /// header name
    pub fn new(auto_generate_span: bool) -> Self {
        Self {
            jaeger_trace_context_header_name: Some(DEFAULT_TRACE_HEADER_NAME.into()),
            auto_generate_span,
        }
    }

    /// specify a header for jaeger_trace_context_header_name
    ///
    /// For example, 'uber-trace-id'
    pub fn with_jaeger_trace_context_header_name(mut self, name: impl AsRef<str>) -> Self {
        self.jaeger_trace_context_header_name = Some(name.as_ref().into());
        self
    }

    /// Create a SpanContext for the trace described in the request's
    /// headers, if any
    ///
    /// Currently support the following formats:
    /// * <https://www.jaegertracing.io/docs/1.21/client-libraries/#propagation-format>
    pub fn parse(
        &self,
        collector: Option<Arc<dyn TraceExporter>>,
        headers: &HeaderMap,
    ) -> Result<Option<SpanContext>, ContextError> {
        if let Some(trace_header) = self.jaeger_trace_context_header_name.as_ref() {
            match (
                headers.contains_key(&**trace_header),
                self.auto_generate_span,
            ) {
                (true, _) => {
                    let decoded =
                        required_header(headers, trace_header.as_ref(), FromStr::from_str)?;
                    return decode_jaeger(collector, decoded).map(Some);
                }
                (false, true) => {
                    return Ok(Some(SpanContext::new_with_optional_collector(collector)));
                }
                _ => return Ok(None),
            }
        }

        Ok(None)
    }

    pub fn parse_str(
        &self,
        collector: Option<Arc<dyn TraceExporter>>,
        trace_header: &str,
        value: Option<String>,
    ) -> Result<Option<SpanContext>, ContextError> {
        match (&value, self.auto_generate_span) {
            (Some(value), _) => {
                let decoded =
                    JaegerCtx::from_str(value).map_err(|err| ContextError::HeaderDecodeError {
                        header: trace_header.to_string(),
                        source: err,
                    })?;
                decode_jaeger(collector, decoded).map(Some)
            }
            (None, true) => Ok(Some(SpanContext::new_with_optional_collector(collector))),
            _ => Ok(None),
        }
    }
}

struct JaegerCtx {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: Option<SpanId>,
    flags: u8,
}

impl FromStr for JaegerCtx {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (trace_id, span_id, parent_span_id, flags) =
            s.split(':')
                .collect_tuple()
                .ok_or(DecodeError::InvalidJaegerTrace {
                    value: s.to_string(),
                })?;

        let trace_id = parse_trace(trace_id)?;
        let span_id = parse_span(span_id)?;
        let parent_span_id = match parse_span(parent_span_id) {
            Ok(span_id) => Some(span_id),
            Err(DecodeError::ZeroError) => None,
            Err(e) => return Err(e),
        };
        let flags = u8::from_str_radix(flags, 16)?;

        Ok(Self {
            trace_id,
            span_id,
            parent_span_id,
            flags,
        })
    }
}

/// Decodes headers in the Jaeger format
fn decode_jaeger(
    collector: Option<Arc<dyn TraceExporter>>,
    decoded: JaegerCtx,
) -> Result<SpanContext, ContextError> {
    let sampled = decoded.flags & 0x01 == 1;

    // Links cannot be specified via the HTTP header
    let links = vec![];

    Ok(SpanContext {
        trace_id: decoded.trace_id,
        parent_span_id: decoded.parent_span_id,
        span_id: decoded.span_id,
        links,
        collector,
        sampled,
    })
}

/// Decodes a given header from the provided HeaderMap to a string
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string
/// - Returns Ok(Some(_)) otherwise
fn decoded_header<'a>(
    headers: &'a HeaderMap,
    header: &str,
) -> Result<Option<&'a str>, ContextError> {
    headers
        .get(header)
        .map(|value| {
            value.to_str().map_err(|source| ContextError::InvalidUtf8 {
                header: header.to_string(),
                source,
            })
        })
        .transpose()
}

/// Decodes and parses a given header from the provided HeaderMap
///
/// - Returns Ok(None) if the header doesn't exist
/// - Returns Err if the header fails to decode to a string or fails to parse
/// - Returns Ok(Some(_)) otherwise
fn parsed_header<T, F: FnOnce(&str) -> Result<T, DecodeError>>(
    headers: &HeaderMap,
    header: &str,
    parse: F,
) -> Result<Option<T>, ContextError> {
    decoded_header(headers, header)?
        .map(parse)
        .transpose()
        .map_err(|source| ContextError::HeaderDecodeError {
            source,
            header: header.to_string(),
        })
}

/// Decodes and parses a given required header from the provided HeaderMap
///
/// - Returns Err if the header fails to decode to a string, fails to parse, or doesn't exist
/// - Returns Ok(str) otherwise
fn required_header<T, F: FnOnce(&str) -> Result<T, DecodeError>>(
    headers: &HeaderMap,
    header: &str,
    parse: F,
) -> Result<T, ContextError> {
    parsed_header(headers, header, parse)?.ok_or(ContextError::Missing {
        header: header.to_string(),
    })
}

/// Format span context as Jaeger trace context.
///
/// This only emits the value-part required for tracer. You must still add the header name to the framework / output
/// stream you're using.
///
/// You may use [`TraceHeaderParser`] to parse the resulting value.
#[allow(clippy::bool_to_int_with_if)] // if sampled 1 else 0 is clearer than i32::from(sampled) imo
pub fn format_jaeger_trace_context(span_context: &SpanContext) -> String {
    let flags = if span_context.sampled { 1 } else { 0 };

    format!(
        "{:x}:{:x}:{:x}:{}",
        span_context.trace_id.get(),
        span_context.span_id.get(),
        span_context
            .parent_span_id
            .as_ref()
            .map(|span_id| span_id.get())
            .unwrap_or_default(),
        flags,
    )
}

/// A simple way to format an external span context in a jaeger-like fashion, e.g. for logging.
pub trait RequestLogContextExt {
    /// Format context.
    fn format_jaeger(&self) -> Option<String>;
}

impl RequestLogContextExt for Option<SpanContext> {
    fn format_jaeger(&self) -> Option<String> {
        self.as_ref().format_jaeger()
    }
}

impl RequestLogContextExt for Option<&SpanContext> {
    fn format_jaeger(&self) -> Option<String> {
        self.map(format_jaeger_trace_context)
    }
}

pub fn append_trace_context(
    span_ctx: impl RequestLogContextExt,
    headers: &mut MetadataMap,
) -> Result<(), InvalidMetadataValue> {
    if let Some(value) = span_ctx.format_jaeger() {
        headers.insert(DEFAULT_TRACE_HEADER_NAME, value.try_into()?);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_decode_jaeger() {
        const TRACE_HEADER: &str = "uber-trace-id";

        let parser =
            TraceHeaderParser::new(false).with_jaeger_trace_context_header_name(TRACE_HEADER);

        let mut headers = HeaderMap::new();

        // Invalid format
        headers.insert(TRACE_HEADER, HeaderValue::from_static("invalid"));
        assert_eq!(
            parser.parse(None, &headers)
                .unwrap_err()
                .to_string(),
            "decoding header 'uber-trace-id': Expected \"trace-id:span-id:parent-span-id:flags\", found: invalid"
        );

        // Not sampled
        headers.insert(TRACE_HEADER, HeaderValue::from_static("343:4325345:0:0"));
        let span = parser.parse(None, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x343);
        assert_eq!(span.span_id.0.get(), 0x4325345);
        assert!(span.parent_span_id.is_none());
        assert!(!span.sampled);

        // Sampled
        headers.insert(TRACE_HEADER, HeaderValue::from_static("3a43:432e345:0:1"));
        let span = parser.parse(None, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x3a43);
        assert_eq!(span.span_id.0.get(), 0x432e345);
        assert!(span.parent_span_id.is_none());
        assert!(span.sampled);

        // Parent span
        headers.insert(TRACE_HEADER, HeaderValue::from_static("343:4325345:3434:F"));
        let span = parser.parse(None, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x343);
        assert_eq!(span.span_id.0.get(), 0x4325345);
        assert_eq!(span.parent_span_id.unwrap().0.get(), 0x3434);
        assert!(span.sampled);

        // Invalid trace id
        headers.insert(TRACE_HEADER, HeaderValue::from_static("0:4325345:3434:1"));
        assert_eq!(
            parser.parse(None, &headers).unwrap_err().to_string(),
            "decoding header 'uber-trace-id': value cannot be 0"
        );

        headers.insert(
            TRACE_HEADER,
            HeaderValue::from_static("008e813572f53b3a:008e813572f53b3a:0000000000000000:1"),
        );

        let span = parser.parse(None, &headers).unwrap().unwrap();

        assert_eq!(span.trace_id.0.get(), 0x008e813572f53b3a);
        assert_eq!(span.span_id.0.get(), 0x008e813572f53b3a);
        assert!(span.parent_span_id.is_none());
        assert!(span.sampled);
    }
}
