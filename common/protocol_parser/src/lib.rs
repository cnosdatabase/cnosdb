extern crate core;

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use protos::FieldValue;
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use utils::BkdrHasher;

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub use json_protocol::parser::Error as JsonLogError;
pub use line_protocol::parser::Error as LineProtocolError;

type NextTagRes<'a> = Result<Option<(Vec<(Cow<'a, str>, Cow<'a, str>)>, usize)>>;

pub mod json_protocol;
pub mod line_protocol;
pub mod lines_convert;
pub mod open_tsdb;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Error: pos: {}, in: '{}'", pos, content))]
    Parse { pos: usize, content: String },

    #[snafu(display("line missing field : {} or line invalid: {}", field, buf))]
    MissingField { field: String, buf: String },

    #[snafu(display("{}", content))]
    Common { content: String },
}

#[derive(Debug, Default, PartialEq)]
pub struct Line<'a> {
    pub hash_id: u64,
    pub table: Cow<'a, str>,
    pub tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
    pub fields: Vec<(Cow<'a, str>, FieldValue)>,
    pub timestamp: i64,
}

impl<'a> From<DataPoint<'a>> for Line<'a> {
    fn from(value: DataPoint<'a>) -> Self {
        let tags = value
            .tags
            .into_iter()
            .map(|(s1, s2)| (Cow::Borrowed(s1), Cow::Borrowed(s2)))
            .collect();
        let fields = vec![(Cow::Borrowed("value"), FieldValue::F64(value.value))];
        let mut line = Line {
            hash_id: 0,
            table: Cow::Borrowed(value.metric),
            tags,
            fields,
            timestamp: value.timestamp,
        };
        line.init_hash_id();
        line
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DataPoint<'a> {
    pub metric: &'a str,
    pub timestamp: i64,
    pub value: f64,
    pub tags: BTreeMap<&'a str, &'a str>,
}

impl<'a> Line<'_> {
    pub fn init_hash_id(&mut self) {
        if self.hash_id == 0 {
            self.tags
                .sort_by(|a, b| -> Ordering { a.0.partial_cmp(&b.0).unwrap() });

            let mut hasher = BkdrHasher::new();
            hasher.hash_with(self.table.as_bytes());
            for (k, v) in &self.tags {
                hasher.hash_with(k.as_bytes());
                hasher.hash_with(v.as_bytes());
            }

            self.hash_id = hasher.number()
        }
    }

    pub fn init_ordered_hash_id(&mut self) {
        if self.hash_id == 0 {
            let mut hasher = BkdrHasher::new();
            hasher.hash_with(self.table.as_bytes());
            for (k, v) in &self.tags {
                hasher.hash_with(k.as_bytes());
                hasher.hash_with(v.as_bytes());
            }

            self.hash_id = hasher.number()
        }
    }

    pub fn new(
        measurement: Cow<'a, str>,
        tags: Vec<(Cow<'a, str>, Cow<'a, str>)>,
        fields: Vec<(Cow<'a, str>, FieldValue)>,
        timestamp: i64,
    ) -> Line<'a> {
        let mut res = Line {
            hash_id: 0,
            table: measurement,
            tags,
            fields,
            timestamp,
        };
        res.init_hash_id();
        res
    }

    pub fn sort_dedup_and_hash(&mut self) {
        self.tags.sort_by(|a, b| a.0.cmp(&b.0));
        self.fields.sort_by(|a, b| a.0.cmp(&b.0));
        self.tags.dedup_by(|a, b| a.0 == b.0);
        self.fields.dedup_by(|a, b| a.0 == b.0);
        self.init_ordered_hash_id();
    }
}

fn check_pos_valid(buf: &str, pos: usize) -> Result<()> {
    if pos < buf.len() {
        return Ok(());
    }
    Err(Error::Parse {
        pos,
        content: String::from(buf),
    })
}

fn next_metric(buf: &str) -> Option<(&str, usize)> {
    let mut escaped = false;
    let mut exists_metric = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    let mut i = 0;
    for c in buf.chars() {
        // Measurement begin character
        if c == '\\' {
            escaped = true;
            if !exists_metric {
                exists_metric = true;
                tok_begin = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_metric {
            // Measurement end character
            if c == ' ' {
                tok_end = i;
                break;
            }
        } else {
            // Measurement begin character
            if c.is_alphanumeric() {
                exists_metric = true;
                tok_begin = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_metric {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

fn is_tagset_character(c: char) -> bool {
    c.is_alphanumeric() || c == '\\'
}

fn next_tag_set(buf: &str) -> NextTagRes {
    let mut escaped = false;
    let mut exists_tag_set = false;
    let mut tok_offsets = [0_usize; 3];
    let mut tok_end = 0_usize;

    let mut tag_set = Vec::new();
    let mut i = 0;
    for c in buf.chars() {
        // TagSet begin character
        if !escaped && c == '\\' {
            escaped = true;
            if !exists_tag_set {
                exists_tag_set = true;
                tok_offsets[0] = i;
            }
            i += c.len_utf8();
            continue;
        }
        if exists_tag_set {
            if !escaped && c == '=' {
                tok_offsets[1] = i;
                if buf.len() <= i + 1 {
                    return Ok(None);
                }
                tok_offsets[2] = i + 1;
            }
            if !escaped && c == ',' {
                tag_set.push((
                    Cow::Borrowed(&buf[tok_offsets[0]..tok_offsets[1]]),
                    Cow::Borrowed(&buf[tok_offsets[2]..i]),
                ));
                if buf.len() <= i + 1 {
                    return Ok(None);
                }
                tok_offsets[0] = i + 1;
            }

            // TagSet end character
            if !escaped && c == ' ' {
                tok_end = i;
                break;
            }
        } else {
            // TagSet begin character
            if is_tagset_character(c) {
                exists_tag_set = true;
                tok_offsets[0] = i;
            }
        }
        if escaped {
            escaped = false;
        }

        i += c.len_utf8();
    }
    if exists_tag_set {
        if tok_end == 0 {
            tok_end = buf.len();
            if buf.ends_with('\n') {
                tok_end -= 1;
            }
        }
        if tok_offsets[0] == tok_offsets[1] {
            return Err(Error::Common {
                content: format!(
                    "tag missing name in next_tag_set, buf : {}",
                    buf.get(0..30).unwrap_or(buf)
                ),
            });
        }
        tag_set.push((
            Cow::Borrowed(&buf[tok_offsets[0]..tok_offsets[1]]),
            Cow::Borrowed(&buf[tok_offsets[2]..tok_end]),
        ));
        Ok(Some((tag_set, tok_end + 1)))
    } else {
        Ok(None)
    }
}

fn next_value(buf: &str) -> Option<(&str, usize)> {
    let mut exists_timestamp = false;
    let (mut tok_begin, mut tok_end) = (0, buf.len());
    for (i, c) in buf.chars().enumerate() {
        if exists_timestamp {
            // Timestamp end character
            if !c.is_numeric() {
                tok_end = i;
                break;
            }
        } else {
            // Timestamp begin character
            if c == '-' || c.is_numeric() {
                tok_begin = i;
                exists_timestamp = true;
            }
        }
    }
    if exists_timestamp {
        Some((&buf[tok_begin..tok_end], tok_end + 1))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use crate::line_protocol::parser::Parser;
    use crate::lines_convert::line_to_point;

    #[test]
    #[ignore]
    fn test_lp_to_flatbuffer() {
        let filename = "/Users/cnosdb/github.com/cnosdb/test_data-iot-seed-123-scale-100";
        let file_data = std::fs::read_to_string(filename).unwrap();

        let t = std::time::Instant::now();
        let parser = Parser::new(100000);
        let lines = parser.parse(&file_data).unwrap();
        println!("parse lines: {} elapsed: {:?}", lines.len(), t.elapsed());

        let t = std::time::Instant::now();
        let flatbuf = line_to_point(&lines, "test_db_12345678").unwrap();
        println!("to flatbuf: {} elapsed: {:?}", flatbuf.len(), t.elapsed());
    }
}
