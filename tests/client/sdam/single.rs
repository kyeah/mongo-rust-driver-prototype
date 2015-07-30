use mongodb::Error::OperationError;
use mongodb::topology::TopologyDescription;
use mongodb::topology::server::ServerDescription;

use std::sync::{Arc, RwLock};
use rustc_serialize::json::Json;

