mod common;
mod opcua_data_provider;

use chrontext::engine::Engine;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use log::debug;
use opcua::server::prelude::*;
use polars::prelude::{DataFrame, SortMultipleOptions};
use rstest::*;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{sleep, JoinHandle};
use std::{thread, time};
use virtualization::timeseries_opcua_database::TimeseriesOPCUADatabase;
use virtualized_query::pushdown_setting::PushdownSetting;
use tokio::runtime::Builder;

use crate::common::{
    add_sparql_testdata, read_csv, start_sparql_container, wipe_database, QUERY_ENDPOINT,
};
use crate::opcua_data_provider::OPCUADataProvider;

#[fixture]
fn use_logger() {
    let res = env_logger::try_init();
    match res {
        Ok(_) => {}
        Err(_) => {
            debug!("Tried to initialize logger which is already initialize")
        }
    }
}

#[fixture]
fn testdata_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut testdata_path = PathBuf::new();
    testdata_path.push(manidir);
    testdata_path.push("tests");
    testdata_path.push("query_execution_opcua");
    testdata_path
}

#[fixture]
fn sparql_endpoint() {
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    runtime.block_on(start_sparql_container());
}

#[fixture]
#[allow(path_statements)]
fn with_testdata(sparql_endpoint: (), testdata_path: PathBuf) {
    sparql_endpoint;
    let mut testdata_path = testdata_path.clone();
    testdata_path.push("testdata.sparql");
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    runtime.block_on(wipe_database());
    runtime.block_on(add_sparql_testdata(testdata_path));
}

#[fixture]
fn frames(testdata_path: PathBuf) -> HashMap<String, DataFrame> {
    let mut frames = HashMap::new();
    for t in ["ts1", "ts2"] {
        let mut file_path = testdata_path.clone();
        file_path.push(t.to_string() + ".csv");

        let df = read_csv(file_path);
        frames.insert(t.to_string(), df);
    }
    frames
}

#[fixture]
fn opcua_server_fixture(frames: HashMap<String, DataFrame>) -> JoinHandle<()> {
    let port = 1234;
    let path = "/";
    //From https://github.com/locka99/opcua/blob/master/docs/server.md
    let server = ServerBuilder::new()
        .application_name("Server Name")
        .application_uri("urn:server_uri")
        .discovery_urls(vec![format!(
            "opc.tcp://{}:{}{}",
            hostname().unwrap(),
            port,
            path
        )])
        .create_sample_keypair(true)
        .pki_dir("./pki-server")
        .discovery_server_url(None)
        .host_and_port(hostname().unwrap(), port)
        .endpoints(
            [(
                "",
                "/",
                SecurityPolicy::None,
                MessageSecurityMode::None,
                &[ANONYMOUS_USER_TOKEN_ID],
            )]
            .iter()
            .map(|v| {
                (
                    v.0.to_string(),
                    ServerEndpoint::from((v.1, v.2, v.3, &v.4[..])),
                )
            })
            .collect(),
        )
        .server()
        .unwrap();
    {
        let server_state = server.server_state();
        let mut server_state = server_state.write();
        server_state.set_historical_data_provider(Box::new(OPCUADataProvider { frames }))
    }
    let handle = thread::spawn(move || server.run());
    sleep(time::Duration::from_secs(2));
    handle
}

#[fixture]
fn engine() -> Engine {
    let port = 1234;
    let path = "/";
    let endpoint = format!("opc.tcp://{}:{}{}", hostname().unwrap(), port, path);
    let opcua_tsdb = TimeseriesOPCUADatabase::new(&endpoint, 1);

    Engine::new(
        [PushdownSetting::GroupBy].into(),
        Arc::new(opcua_tsdb),
        Arc::new(SparqlEndpoint {
            endpoint: QUERY_ENDPOINT.to_string(),
        }),
    )
}

#[rstest]
#[serial]
#[allow(path_statements)]
fn test_basic_query(
    with_testdata: (),
    use_logger: (),
    opcua_server_fixture: JoinHandle<()>,
    testdata_path: PathBuf,
    engine: Engine,
) {
    with_testdata;
    use_logger;
    let _ = opcua_server_fixture;

    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t >= "2022-06-01T08:46:53"^^xsd:dateTime && ?t <= "2022-06-01T08:46:58"^^xsd:dateTime) .
    }
    "#;
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    let df = runtime
        .block_on(engine.query(query))
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_basic_query.csv");
    let mut expected_df = read_csv(file_path);
    expected_df
        .with_column(
            expected_df
                .column("t")
                .unwrap()
                .cast(&polars::prelude::DataType::Datetime(
                    polars::prelude::TimeUnit::Milliseconds,
                    None,
                ))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(expected_df, df);
    //
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    //println!("{}", df);
}

#[rstest]
#[serial]
#[allow(path_statements)]
fn test_basic_no_end_time_query(
    with_testdata: (),
    use_logger: (),
    opcua_server_fixture: JoinHandle<()>,
    testdata_path: PathBuf,
    engine: Engine,
) {
    with_testdata;
    use_logger;
    let _ = opcua_server_fixture;

    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t >= "2022-06-01T08:46:54"^^xsd:dateTime) .
    }
    "#;
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    let df = runtime
        .block_on(engine.query(query))
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_basic_no_end_time_query.csv");
    let mut expected_df = read_csv(file_path);
    expected_df
        .with_column(
            expected_df
                .column("t")
                .unwrap()
                .cast(&polars::prelude::DataType::Datetime(
                    polars::prelude::TimeUnit::Milliseconds,
                    None,
                ))
                .unwrap(),
        )
        .unwrap();
    assert_eq!(expected_df, df);

    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[serial]
#[allow(path_statements)]
fn test_pushdown_group_by_five_second_hybrid_query(
    with_testdata: (),
    use_logger: (),
    opcua_server_fixture: JoinHandle<()>,
    testdata_path: PathBuf,
    engine: Engine,
) {
    with_testdata;
    use_logger;
    let _ = opcua_server_fixture;

    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?datetime_seconds (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(5 * FLOOR(chrontext:DateTimeAsSeconds(?t) / 5) as ?datetime_seconds)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?datetime_seconds
    "#;
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    let mut df = runtime
        .block_on(engine.query(query))
        .expect("Hybrid error")
        .0;
    df = df
        .sort(
            vec!["w", "datetime_seconds"],
            SortMultipleOptions::default(),
        )
        .unwrap();
    df.with_column(
        df.column("datetime_seconds")
            .unwrap()
            .cast(&polars::prelude::DataType::Datetime(
                polars::prelude::TimeUnit::Milliseconds,
                None,
            ))
            .unwrap(),
    )
    .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_five_second_hybrid_query.csv");
    let mut expected_df = read_csv(file_path);
    expected_df
        .with_column(
            expected_df
                .column("datetime_seconds")
                .unwrap()
                .cast(&polars::prelude::DataType::Datetime(
                    polars::prelude::TimeUnit::Milliseconds,
                    None,
                ))
                .unwrap(),
        )
        .unwrap();
    expected_df = expected_df
        .sort(
            vec!["w", "datetime_seconds"],
            SortMultipleOptions::default(),
        )
        .unwrap();

    assert_eq!(expected_df, df);

    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[serial]
#[allow(path_statements)]
fn test_no_pushdown_because_of_filter_query(
    with_testdata: (),
    use_logger: (),
    opcua_server_fixture: JoinHandle<()>,
    testdata_path: PathBuf,
    engine: Engine,
) {
    with_testdata;
    use_logger;
    let _ = opcua_server_fixture;

    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?datetime_seconds (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(xsd:integer(5 * FLOOR(chrontext:DateTimeAsSeconds(?t) / 5.0)) as ?datetime_seconds)
        FILTER(?v > 100 && ?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?datetime_seconds
    "#;
    let mut builder = Builder::new_multi_thread();
    builder.enable_all();
    let runtime = builder.build().unwrap();
    let mut df = runtime
        .block_on(engine.query(query))
        .expect("Hybrid error")
        .0;
    df = df
        .sort(
            vec!["w", "datetime_seconds"],
            SortMultipleOptions::default(),
        )
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_no_pushdown_because_of_filter_query.csv");
    let mut expected_df = read_csv(file_path);
    expected_df = expected_df
        .sort(
            vec!["w", "datetime_seconds"],
            SortMultipleOptions::default(),
        )
        .unwrap();

    assert_eq!(expected_df, df);
}

//
//     let file = File::create(file_path.as_path()).expect("could not open file");
//     let mut writer = CsvWriter::new(file);
//     writer.finish(&mut df).expect("writeok");
//     println!("{}", df);
//
