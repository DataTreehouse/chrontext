mod common;

use chrontext::engine::Engine;
use chrontext::pushdown_setting::all_pushdowns;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use chrontext::sparql_database::SparqlQueryable;
use chrontext::splitter::parse_sparql_select_query;
use chrontext::timeseries_database::timeseries_in_memory_database::TimeseriesInMemoryDatabase;
use log::debug;
use oxrdf::{NamedNode, Term, Variable};
use polars::prelude::{CsvReader, SerReader};
use rstest::*;
use serial_test::serial;
use sparesults::QuerySolution;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use crate::common::{
    add_sparql_testdata, compare_all_solutions, start_sparql_container, QUERY_ENDPOINT,
};

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
    testdata_path.push("query_execution_testdata");
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
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
fn inmem_time_series_database(testdata_path: PathBuf) -> TimeseriesInMemoryDatabase {
    let mut frames = HashMap::new();
    for t in ["ts1", "ts2"] {
        let mut file_path = testdata_path.clone();
        file_path.push(t.to_string() + ".csv");

        let file = File::open(file_path.as_path()).expect("could not open file");
        let df = CsvReader::new(file)
            .infer_schema(None)
            .has_header(true)
            .with_try_parse_dates(true)
            .finish()
            .expect("DF read error");
        frames.insert(t.to_string(), df);
    }
    TimeseriesInMemoryDatabase { frames }
}

#[fixture]
fn engine(inmem_time_series_database: TimeseriesInMemoryDatabase) -> Engine {
    Engine::new(
        all_pushdowns(),
        Box::new(inmem_time_series_database),
        Box::new(SparqlEndpoint {
            endpoint: QUERY_ENDPOINT.to_string(),
        }),
    )
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_static_query(#[future] with_testdata: (), use_logger: ()) {
    use_logger;
    let _ = with_testdata.await;
    let query = parse_sparql_select_query(
        r#"
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
    SELECT * WHERE {?a chrontext:hasTimeseries ?b }
    "#,
    )
    .unwrap();
    let mut ep = SparqlEndpoint {
        endpoint: QUERY_ENDPOINT.to_string(),
    };
    let query_solns = ep.execute(&query).await.unwrap();
    let expected_solutions = vec![
        QuerySolution::from((
            vec![Variable::new("a").unwrap(), Variable::new("b").unwrap()],
            vec![
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#mySensor2").unwrap(),
                )),
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#myTimeseries2").unwrap(),
                )),
            ],
        )),
        QuerySolution::from((
            vec![Variable::new("a").unwrap(), Variable::new("b").unwrap()],
            vec![
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#mySensor1").unwrap(),
                )),
                Some(Term::NamedNode(
                    NamedNode::new("http://example.org/case#myTimeseries1").unwrap(),
                )),
            ],
        )),
    ];
    compare_all_solutions(expected_solutions, query_solns);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
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
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_complex_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w1 ?w2 ?t ?v1 ?v2 WHERE {
        ?w1 a types:BigWidget .
        ?w2 a types:SmallWidget .
        ?w1 types:hasSensor ?s1 .
        ?w2 types:hasSensor ?s2 .
        ?s1 chrontext:hasTimeseries ?ts1 .
        ?s2 chrontext:hasTimeseries ?ts2 .
        ?ts1 chrontext:hasDataPoint ?dp1 .
        ?ts2 chrontext:hasDataPoint ?dp2 .
        ?dp1 chrontext:hasTimestamp ?t .
        ?dp2 chrontext:hasTimestamp ?t .
        ?dp1 chrontext:hasValue ?v1 .
        ?dp2 chrontext:hasValue ?v2 .
        FILTER(?t > "2022-06-01T08:46:55"^^xsd:dateTime && ?v1 < ?v2) .
    }
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_complex_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_pushdown_group_by_hybrid_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime) .
    } GROUP BY ?w
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error")
        .sort(["w"], vec![false], false)
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error")
        .sort(["w"], vec![false], false)
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}
