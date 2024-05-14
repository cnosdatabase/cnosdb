use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct SqlParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub chunked: Option<bool>,
    // Number of partitions for query execution. Increasing partitions can increase concurrency.
    pub target_partitions: Option<usize>,
    pub stream_trigger_interval: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct WriteParam {
    pub precision: Option<String>,
    pub tenant: Option<String>,
    pub db: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DumpParam {
    pub tenant: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebugParam {
    pub id: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ESLogParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub table: Option<String>,
    pub have_es_command: Option<bool>,
    pub tag_columns: Option<String>,
    pub time_column: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PointCloudParam {
    pub tenant: Option<String>,
    pub db: Option<String>,
    pub file: Option<String>,
}
