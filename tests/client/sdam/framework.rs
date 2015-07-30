#[macro_export]
macro_rules! run_suite {
    ( $file:expr ) => {{
        let json = Json::from_file($file).unwrap();
        let mut suite = json.get_suite().unwrap();

        let mut topology_description = TopologyDescription::new();
        let mut server_description = ServerDescription::new();

        let dummy_top_description = Arc::new(RwLock::new(TopologyDescription::new()));
        let dummy_req_id = Arc::new(AtomicIsize::new());
        
        for phase in suite.phases {            
            for (host, response) in phase.operation.data {
                if response.is_empty() {
                    server_description.set_err(OperationError("Simulated network error.".to_owned()));
                } else {
                    match IsMasterResult::new(response) {
                        Ok(ismaster) => server_description.update(ismaster),
                        _ => panic!("Failed to parse ismaster result."),
                    }
                    
                    topology_description.update(host.clone(), server_description.clone(),
                                                dummy_req_id.clone(), dummy_top_description.clone());
                }
            }

            assert_eq!(phase.outcome.len(), topology_description.servers.len());
            for (host, server) in phase.outcome.iter() {
                match topology_description.servers.get(host) {
                    Some(top_server) => {
                        assert_eq!(server.set_name, top_server.set_name);
                        assert_eq!(server.stype, top_server.stype);
                    },
                    None => panic!("Missing host in outcome."),
                }
            }

            assert_eq!(phase.outcome.set_name, topology_description.set_name);
            assert_eq!(phase.outcome.ttype, topology_description.ttype);
        }
    }};
}
