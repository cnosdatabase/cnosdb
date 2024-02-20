#![cfg(test)]
#![warn(dead_code)]
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use serial_test::serial;

use crate::utils::{build_data_node_config, get_workspace_dir, kill_all, run_singleton};
use crate::{assert_response_is_ok, cluster_def};

#[test]
#[serial]
// fix for 1814
fn divisor_test() {
    println!("Test begin auth_test");

    let test_dir = "/tmp/e2e_test/flush_tests/divisor_test";
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();

    kill_all();

    let data_node_def = &cluster_def::one_data(1);

    {
        let _data = run_singleton(test_dir, data_node_def, false, true);
    }

    // change cache size max_buffer_size=2M
    let mut config = build_data_node_config(test_dir, &data_node_def.config_file_name);
    data_node_def.update_config(&mut config);

    config.cache.max_buffer_size = 2 * 1024 * 1024;
    let config_dir = Path::new(test_dir).join("data").join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_file_path = config_dir.join(&data_node_def.config_file_name);
    std::fs::write(config_file_path, config.to_string_pretty()).unwrap();

    let server = run_singleton(test_dir, data_node_def, false, false);

    let data_dir = get_workspace_dir().join("e2e_test").join("test_data");

    {
        let mut buf = String::new();
        let mut file = BufReader::new(File::open(data_dir.join("log_0.txt")).unwrap());
        file.read_to_string(&mut buf).unwrap();
        let resp = server
            .client
            .post("http://127.0.0.1:8902/api/v1/write?db=public", &buf)
            .unwrap();

        assert_response_is_ok!(resp);
    }

    {
        let mut buf = String::new();
        let mut file = BufReader::new(File::open(data_dir.join("log_1.txt")).unwrap());
        file.read_to_string(&mut buf).unwrap();
        let resp = server
            .client
            .post("http://127.0.0.1:8902/api/v1/write?db=public", &buf)
            .unwrap();

        assert_response_is_ok!(resp);
    }
    let resp = server
        .client
        .post(
            "http://127.0.0.1:8902/api/v1/sql?db=public",
            "select count(*) from log;",
        )
        .unwrap();
    assert_response_is_ok!(resp);

    assert_eq!(resp.text().unwrap(), "COUNT(UInt8(1))\n17280\n")
}
