mod common;
mod timeseries_in_memory_database;

use chrontext::engine::Engine;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use log::debug;
use polars::prelude::SortMultipleOptions;
use rstest::*;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use timeseries_query::pushdown_setting::all_pushdowns;

use crate::common::{
    add_sparql_testdata, read_csv, start_sparql_container, wipe_database, QUERY_ENDPOINT,
};
use crate::timeseries_in_memory_database::TimeseriesInMemoryDatabase;

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
    testdata_path.push("query_execution_syntactic_sugar");
    testdata_path
}

#[fixture]
async fn sparql_endpoint() {
    start_sparql_container().await
}

#[fixture]
async fn with_testdata(#[future] sparql_endpoint: (), testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    let mut testdata_path = testdata_path.clone();
    testdata_path.push("testdata.sparql");
    wipe_database().await;
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
fn inmem_time_series_database(testdata_path: PathBuf) -> TimeseriesInMemoryDatabase {
    let mut frames = HashMap::new();
    for t in ["ts1", "ts2"] {
        let mut file_path = testdata_path.clone();
        file_path.push(t.to_string() + ".csv");

        let df = read_csv(file_path);
        frames.insert(t.to_string(), df);
    }
    TimeseriesInMemoryDatabase { frames }
}

#[fixture]
fn engine(inmem_time_series_database: TimeseriesInMemoryDatabase) -> Engine {
    Engine::new(
        all_pushdowns(),
        Arc::new(inmem_time_series_database),
        Arc::new(SparqlEndpoint {
            endpoint: QUERY_ENDPOINT.to_string(),
        }),
    )
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_simple_hybrid_query_sugar(
    #[future] with_testdata: (),
    engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        DT {
         from = "2022-06-01T08:46:53Z",
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid_sugar.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_simple_hybrid_query_sugar_timeseries_explicit_link(
    #[future] with_testdata: (),
    engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    #[allow(path_statements)]
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        DT {
         timeseries = ?ts,
         from = "2022-06-01T08:46:53Z",
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid_sugar.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_simple_hybrid_query_sugar_agg_avg(
    #[future] with_testdata: (),
    engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    #[allow(path_statements)]
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        DT {
         from = "2022-06-01T08:46:53Z",
         aggregation = "avg",
         interval = "5s",
        }
    }
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "s", "timestamp"], SortMultipleOptions::default())
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid_sugar_agg_avg.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}
