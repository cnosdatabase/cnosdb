use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::override_by_env::{entry_override, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HintedOffConfig {
    #[serde(default = "HintedOffConfig::default_enable")]
    pub enable: bool,
    #[serde(default = "HintedOffConfig::default_path")]
    pub path: String,
    #[serde(default = "HintedOffConfig::default_threads")]
    pub threads: i32,
}

impl HintedOffConfig {
    fn default_enable() -> bool {
        true
    }

    fn default_path() -> String {
        "/var/lib/cnosdb/hh".to_string()
    }

    fn default_threads() -> i32 {
        3
    }
}

impl OverrideByEnv for HintedOffConfig {
    fn override_by_env(&mut self) {
        entry_override(&mut self.enable, "CNOSDB_HINTED_OFF_ENABLE");
        entry_override(&mut self.path, "CNOSDB_HINTED_OFF_PATH");
        entry_override(&mut self.threads, "CNOSDB_HINTED_OFF_THREADS");
    }
}

impl Default for HintedOffConfig {
    fn default() -> Self {
        Self {
            enable: Self::default_enable(),
            path: Self::default_path(),
            threads: Self::default_threads(),
        }
    }
}

impl CheckConfig for HintedOffConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("hinted_off".to_string());
        let mut ret = CheckConfigResult::default();

        if self.enable && self.path.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "path".to_string(),
                message: "'path' is empty".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
