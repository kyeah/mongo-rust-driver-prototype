use mongodb::Error::OperationError;
use mongodb::topology::TopologyDescription;
use mongodb::topology::monitor::IsMasterResult;
use mongodb::topology::server::ServerDescription;

use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicIsize;
use rustc_serialize::json::Json;

#[test]
fn sdam_single() {
    let paths = fs::read_dir(&Path::new("tests/json/data/specs/source/server-discovery-and-monitoring/tests/single/")).unwrap();

    for path in paths {
        let filename = path.unwrap().path().to_string_lossy();
        if filename.ends_with(".json") {
            run_suite!(&filename)
        }
    }
}
