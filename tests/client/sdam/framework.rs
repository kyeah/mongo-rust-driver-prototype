#[macro_export]
macro_rules! run_suite {
    ( $file:expr ) => {{
        let json = Json::from_file(&file).unwrap();
        let mut suite = json.get_suite().unwrap();

        let topology_description = TopologyDescription::new();
        let mut server_description = ServerDescription::new();

        let dummy_top_description = Arc::new(RwLock::new(TopologyDescription::new()));

        for phase in suite.phases {
            for (host, response) in phase.operation.data {
                if response.is_empty() {
                    // Simulate network error
                    server_description.set_err(OperationError("Simulated network error."));
                } else {
                    match IsMasterResult::new(response) {
                        Ok(ismaster) => server_description.update(ismaster),
                        _ => panic!("Failed to parse ismaster result."),
                    }
                    
                    topology_description.update(host.clone(), description.clone(),
                                                req_id.clone(), dummy_top_description.clone());
                }
            }
        }
    }};
}
