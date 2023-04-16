use std::str::FromStr;

use datafusion::arrow::csv::writer::WriterBuilder;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::json::{ArrayWriter, LineDelimitedWriter};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use http_protocol::header::{
    APPLICATION_CSV, APPLICATION_JSON, APPLICATION_NDJSON, APPLICATION_PREFIX, APPLICATION_STAR,
    APPLICATION_TABLE, APPLICATION_TSV, CONTENT_TYPE, STAR_STAR,
};
use http_protocol::status_code::OK;
use warp::reply::Response;
use warp::{reject, Rejection};

use super::Error as HttpError;
use crate::http::header::Header;
use crate::http::response::ResponseBuilder;

macro_rules! batches_to_json {
    ($WRITER: ident, $batches: expr) => {{
        let mut bytes = vec![];
        {
            let mut writer = $WRITER::new(&mut bytes);
            writer.write_batches($batches)?;
            writer.finish()?;
        }
        Ok(bytes)
    }};
}

fn batches_with_sep(batches: &[RecordBatch], delimiter: u8) -> ArrowResult<Vec<u8>> {
    let mut bytes = vec![];
    {
        let builder = WriterBuilder::new()
            .has_headers(true)
            .with_delimiter(delimiter);
        let mut writer = builder.build(&mut bytes);
        for batch in batches {
            writer.write(batch)?;
        }
    }
    Ok(bytes)
}

/// Allow records to be printed in different formats
#[derive(Debug, PartialEq, Eq, clap::ValueEnum, Clone)]
pub enum ResultFormat {
    Csv,
    Tsv,
    Json,
    NdJson,
    Table,
}

impl ResultFormat {
    fn get_http_content_type(&self) -> &'static str {
        match self {
            Self::Csv => APPLICATION_CSV,
            Self::Tsv => APPLICATION_TSV,
            Self::Json => APPLICATION_JSON,
            Self::NdJson => APPLICATION_NDJSON,
            Self::Table => APPLICATION_TABLE,
        }
    }

    pub fn format_batches(&self, batches: &[RecordBatch]) -> ArrowResult<Vec<u8>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }
        match self {
            Self::Csv => batches_with_sep(batches, b','),
            Self::Tsv => batches_with_sep(batches, b'\t'),
            Self::Json => batches_to_json!(ArrayWriter, batches),
            Self::NdJson => {
                batches_to_json!(LineDelimitedWriter, batches)
            }
            Self::Table => Ok(pretty_format_batches(batches)?.to_string().into_bytes()),
        }
    }

    pub fn wrap_batches_to_response(&self, batches: &[RecordBatch]) -> Result<Response, HttpError> {
        let result = self
            .format_batches(batches)
            .map_err(|e| HttpError::FetchResult {
                reason: format!("{}", e),
            })?;

        let resp = ResponseBuilder::new(OK)
            .insert_header((CONTENT_TYPE, self.get_http_content_type()))
            .build(result);

        Ok(resp)
    }
}

impl TryFrom<&str> for ResultFormat {
    type Error = HttpError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() || s == APPLICATION_STAR || s == STAR_STAR {
            return Ok(ResultFormat::Csv);
        }

        if let Some(fmt) = s.strip_prefix(APPLICATION_PREFIX) {
            return ResultFormat::from_str(fmt)
                .map_err(|reason| HttpError::InvalidHeader { reason });
        }

        Err(HttpError::InvalidHeader {
            reason: format!("accept type not support: {}", s),
        })
    }
}

impl FromStr for ResultFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

pub fn get_result_format_from_header(header: &Header) -> Result<ResultFormat, Rejection> {
    ResultFormat::try_from(header.get_accept()).map_err(reject::custom)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::from_slice::FromSlice;

    use super::*;

    #[test]
    fn test_format_batches_with_sep() {
        let batches = vec![];
        assert_eq!("".as_bytes(), batches_with_sep(&batches, b',').unwrap());

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r = batches_with_sep(&batches, b',').unwrap();
        assert_eq!("a,b,c\n1,4,7\n2,5,8\n3,6,9\n".as_bytes(), r);
    }

    #[test]
    fn test_format_batches_to_json_empty() -> ArrowResult<()> {
        let batches = vec![];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!("".as_bytes(), r?);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )
        .unwrap();

        let batches = vec![batch];
        let r: ArrowResult<Vec<u8>> = batches_to_json!(ArrayWriter, &batches);
        assert_eq!(
            "[{\"a\":1,\"b\":4,\"c\":7},{\"a\":2,\"b\":5,\"c\":8},{\"a\":3,\"b\":6,\"c\":9}]"
                .as_bytes(),
            r?
        );

        let r: ArrowResult<Vec<u8>> = batches_to_json!(LineDelimitedWriter, &batches);
        assert_eq!(
            "{\"a\":1,\"b\":4,\"c\":7}\n{\"a\":2,\"b\":5,\"c\":8}\n{\"a\":3,\"b\":6,\"c\":9}\n"
                .as_bytes(),
            r?
        );
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::basic::Compression;
    use datafusion::parquet::file::properties::WriterProperties;

    // use protocol_parser::lines_convert::parse_lines_to_batch;
    use crate::http::http_service::construct_write_lines_points_request;

    const LINE_PROTOCOL_FILE: &str = "/Users/adminliu/test_data/test_line_pro.txt";

    #[tokio::test]
    #[ignore]
    async fn test1() {
        let data = std::fs::read_to_string(LINE_PROTOCOL_FILE).unwrap();

        let points = construct_write_lines_points_request(
            data.clone().into(),
            "cnosdb_tenant.test_db_test_db_name",
        )
        .unwrap();

        let src1: Vec<&[u8]> = vec![data.as_bytes()];
        let mut dst1 = vec![];
        tskv::tsm::codec::string::str_snappy_encode(&src1, &mut dst1).unwrap();

        let src2: Vec<&[u8]> = vec![&points.points];
        let mut dst2 = vec![];
        tskv::tsm::codec::string::str_snappy_encode(&src2, &mut dst2).unwrap();

        print!(
            "=== lp: {} {} fb: {} {}  ",
            data.len(),
            dst1.len(),
            points.points.len(),
            dst2.len()
        );
    }

    // #[tokio::test]
    // #[ignore]
    // async fn test2() {
    //     let data = std::fs::read_to_string(LINE_PROTOCOL_FILE).unwrap();
    //
    //     let lines = line_protocol_to_lines(&data, Local::now().timestamp_nanos()).unwrap();
    //     let batches = parse_lines_to_batch(&lines).unwrap();
    //
    //     //write_record_batch_to_parquet_file(&batches[0], &format!("{}.pqt", LINE_PROTOCOL_FILE));
    //
    //     for batch in batches {
    //         print_record_size(&batch);
    //     }
    // }

    fn print_record_size(record: &datafusion::arrow::record_batch::RecordBatch) {
        let data = models::record_batch_encode(record).unwrap();
        let src1: Vec<&[u8]> = vec![&data];
        let mut dst1 = vec![];
        tskv::tsm::codec::string::str_zstd_encode(&src1, &mut dst1).unwrap();
        println!(
            "==== print_record_size count: {}, len:{}, {}",
            record.num_rows(),
            data.len(),
            dst1.len()
        );
    }

    fn write_record_batch_to_parquet_file(rb: &RecordBatch, file_path: &str) {
        let option = WriterProperties::builder()
            //.set_bloom_filter_enabled(true)
            .set_compression(Compression::SNAPPY)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, rb.schema(), Some(option)).unwrap();
        writer.write(rb).unwrap();
        writer.close().unwrap();
        println!("=== parquet_file size: {}", buffer.len());

        std::fs::write(file_path, buffer).unwrap();
    }
}
