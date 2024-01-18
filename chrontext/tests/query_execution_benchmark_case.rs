mod common;

use chrontext::engine::Engine;
use chrontext::pushdown_setting::all_pushdowns;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use chrontext::timeseries_database::timeseries_in_memory_database::TimeseriesInMemoryDatabase;
use log::debug;
use polars::prelude::{CsvReader, SerReader};
use rstest::*;
use serial_test::serial;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

use crate::common::{add_sparql_testdata, start_sparql_container, QUERY_ENDPOINT, wipe_database};

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
    testdata_path.push("query_execution_benchmark_case");
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
    for t in [
        "ep1", "ep2", "ep3", "ep4", "ep5", "ep6", "ep7", "ep8", "wsp1", "wsp2", "wsp3", "wsp4",
        "wsp5", "wsp6", "wsp7", "wsp8", "wdir1", "wdir2", "wdir3", "wdir4", "wdir5", "wdir6",
        "wdir7", "wdir8",
    ] {
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
async fn test_should_pushdown_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
PREFIX wp:<https://github.com/DataTreehouse/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/DataTreehouse/chrontext/rds_power#>
SELECT ?site_label ?wtur_label ?year ?month ?day ?hour ?minute_10 (AVG(?val) as ?avg_val) WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?wtur a rds:A .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys a rds:RA .
    ?gensys rds:hasFunctionalAspect ?generator_asp .
    ?generator rds:hasFunctionalAspectNode ?generator_asp .
    ?generator a rds:GAA .
    ?generator ct:hasTimeseries ?ts .
    ?ts rdfs:label "Production" .
    ?ts ct:hasDataPoint ?dp .
    ?dp ct:hasValue ?val .
    ?dp ct:hasTimestamp ?t .
    BIND(10 * FLOOR(minutes(?t) / 10.0) as ?minute_10)
    BIND(hours(?t) AS ?hour)
    BIND(day(?t) AS ?day)
    BIND(month(?t) AS ?month)
    BIND(year(?t) AS ?year)
    FILTER(?site_label = "Wind Mountain"
        && ?wtur_label = "A1"
        && ?t >= "2022-08-30T08:46:53"^^xsd:dateTime
        && ?t <= "2022-08-30T21:46:53"^^xsd:dateTime) .
}
GROUP BY ?site_label ?wtur_label ?year ?month ?day ?hour ?minute_10
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error").0
        .sort(
            vec![
                "site_label",
                "wtur_label",
                "year",
                "month",
                "day",
                "hour",
                "minute_10",
            ],
            false,
            false,
        )
        .unwrap();

    let mut file_path = testdata_path.clone();
    file_path.push("expected_should_pushdown.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let mut expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error");
    for c in df.get_columns() {
        expected_df
            .with_column(
                expected_df
                    .column(c.name())
                    .unwrap()
                    .cast(c.dtype())
                    .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_multi_should_pushdown_query(
    #[future] with_testdata: (),
    mut engine: Engine,
    testdata_path: PathBuf,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
PREFIX wp:<https://github.com/DataTreehouse/chrontext/windpower_example#>
PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rds:<https://github.com/DataTreehouse/chrontext/rds_power#>
SELECT ?site_label ?wtur_label ?year ?month ?day ?hour ?minute_10 (AVG(?val_prod) as ?val_prod_avg) (AVG(?val_dir) as ?val_dir_avg) (AVG(?val_speed) as ?val_speed_avg) WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur a rds:A .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys a rds:RA .
    ?gensys rds:hasFunctionalAspect ?generator_asp .
    ?generator rds:hasFunctionalAspectNode ?generator_asp .
    ?generator a rds:GAA .
    ?wtur rds:hasFunctionalAspect ?weather_asp .
    ?weather rds:hasFunctionalAspectNode ?weather_asp .
    ?weather a rds:LE .
    ?weather ct:hasTimeseries ?ts_speed .
    ?ts_speed ct:hasDataPoint ?dp_speed .
    ?dp_speed ct:hasValue ?val_speed .
    ?dp_speed ct:hasTimestamp ?t .
    ?ts_speed rdfs:label "Windspeed" .
    ?weather ct:hasTimeseries ?ts_dir .
    ?ts_dir ct:hasDataPoint ?dp_dir .
    ?dp_dir ct:hasValue ?val_dir .
    ?dp_dir ct:hasTimestamp ?t .
    ?ts_dir rdfs:label "WindDirection" .
    ?generator ct:hasTimeseries ?ts_prod .
    ?ts_prod rdfs:label "Production" .
    ?ts_prod ct:hasDataPoint ?dp_prod .
    ?dp_prod ct:hasValue ?val_prod .
    ?dp_prod ct:hasTimestamp ?t .
    BIND(10 * FLOOR(minutes(?t) / 10.0) as ?minute_10)
    BIND(hours(?t) AS ?hour)
    BIND(day(?t) AS ?day)
    BIND(month(?t) AS ?month)
    BIND(year(?t) AS ?year)
    FILTER(?t >= "2022-08-30T08:46:53"^^xsd:dateTime
    && ?t <= "2022-08-30T21:46:53"^^xsd:dateTime) .
}
GROUP BY ?site_label ?wtur_label ?year ?month ?day ?hour ?minute_10
    "#;
    let df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error").0
        .sort(
            vec![
                "site_label",
                "wtur_label",
                "year",
                "month",
                "day",
                "hour",
                "minute_10",
            ],
            false,
            false,
        )
        .unwrap();

    let mut file_path = testdata_path.clone();
    file_path.push("expected_multi_should_pushdown.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let mut expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error");
    for c in df.get_columns() {
        expected_df
            .with_column(
                expected_df
                    .column(c.name())
                    .unwrap()
                    .cast(c.dtype())
                    .unwrap(),
            )
            .unwrap();
    }
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}
