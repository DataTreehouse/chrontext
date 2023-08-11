mod common;

use crate::common::{add_sparql_testdata, find_container, start_sparql_container, QUERY_ENDPOINT};
use bollard::container::{
    Config, CreateContainerOptions, RemoveContainerOptions, StartContainerOptions,
};
use bollard::image::BuildImageOptions;
use bollard::models::{HostConfig, PortBinding};
use bollard::Docker;
use futures_util::stream::StreamExt;
use chrontext::engine::Engine;
use chrontext::pushdown_setting::all_pushdowns;
use chrontext::timeseries_database::arrow_flight_sql_database::ArrowFlightSQLDatabase;
use chrontext::timeseries_database::timeseries_sql_rewrite::TimeSeriesTable;
use log::debug;
use oxrdf::vocab::xsd;
use polars::prelude::{CsvReader, SerReader};
use polars_core::datatypes::DataType;
use polars_core::prelude::TimeUnit;
use reqwest::header::CONTENT_TYPE;
use reqwest::Method;
use rstest::*;
use serde::{Deserialize, Serialize};
use serial_test::serial;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

const ARROW_SQL_DATABASE_ENDPOINT: &str = "grpc+tcp://127.0.0.1:32010";
const DREMIO_ORIGIN: &str = "http://127.0.0.1:9047";

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
    testdata_path.push("query_execution_arrow_sql_testdata");
    testdata_path
}

#[fixture]
fn shared_testdata_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut testdata_path = PathBuf::new();
    testdata_path.push(manidir);
    testdata_path.push("tests");
    testdata_path.push("query_execution_testdata");
    testdata_path
}

#[fixture]
fn dockerfile_tar_gz_path() -> PathBuf {
    let manidir = env!("CARGO_MANIFEST_DIR");
    let mut dockerfile_path = PathBuf::new();
    dockerfile_path.push(manidir);
    dockerfile_path.push("tests");
    dockerfile_path.push("dremio_docker.tar.gz");
    dockerfile_path
}

#[fixture]
async fn sparql_endpoint() {
    start_sparql_container().await;
}

#[fixture]
async fn with_testdata(#[future] sparql_endpoint: (), shared_testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    let mut testdata_path = shared_testdata_path.clone();
    testdata_path.push("testdata.sparql");
    add_sparql_testdata(testdata_path).await;
}

#[fixture]
async fn arrow_sql_endpoint(dockerfile_tar_gz_path: PathBuf) {
    let docker = Docker::connect_with_local_defaults().expect("Could not find local docker");
    let container_name = "my-dremio-server";
    let existing = find_container(&docker, container_name).await;
    if let Some(_) = existing {
        docker
            .remove_container(
                container_name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await
            .expect("Remove existing problem");
    }
    let mut file = File::open(dockerfile_tar_gz_path).unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    let mut build_stream = docker.build_image(
        BuildImageOptions {
            dockerfile: "Dockerfile",
            t: "my_dremio",
            ..Default::default()
        },
        None,
        Some(contents.into()),
    );
    while let Some(msg) = build_stream.next().await {
        println!("Message: {:?}", msg);
    }

    let options = CreateContainerOptions {
        name: container_name,
    };
    let config = Config {
        image: Some("my_dremio"),
        cmd: None,
        exposed_ports: Some(HashMap::from([
            ("9047/tcp", HashMap::new()),
            ("32010/tcp", HashMap::new()),
            ("45678/tcp", HashMap::new()),
        ])),
        host_config: Some(HostConfig {
            port_bindings: Some(HashMap::from([
                (
                    "9047/tcp".to_string(),
                    Some(vec![PortBinding {
                        host_ip: None,
                        host_port: Some("9047/tcp".to_string()),
                    }]),
                ),
                (
                    "32010/tcp".to_string(),
                    Some(vec![PortBinding {
                        host_ip: None,
                        host_port: Some("32010/tcp".to_string()),
                    }]),
                ),
                (
                    "45678/tcp".to_string(),
                    Some(vec![PortBinding {
                        host_ip: None,
                        host_port: Some("45678/tcp".to_string()),
                    }]),
                ),
            ])),
            ..Default::default()
        }),
        ..Default::default()
    };
    docker
        .create_container(Some(options), config)
        .await
        .expect("Problem creating container");
    sleep(Duration::from_secs(1)).await;
    docker
        .start_container(container_name, None::<StartContainerOptions<String>>)
        .await
        .expect("Started container problem ");
    sleep(Duration::from_secs(45)).await;
    let created = find_container(&docker, container_name).await;
    assert!(created.is_some());

    assert!(created
        .as_ref()
        .unwrap()
        .status
        .as_ref()
        .unwrap()
        .contains("Up"));
}

#[fixture]
async fn with_sparql_testdata(#[future] sparql_endpoint: (), mut shared_testdata_path: PathBuf) {
    let _ = sparql_endpoint.await;
    shared_testdata_path.push("testdata.sparql");
    add_sparql_testdata(shared_testdata_path).await;
}

#[fixture]
fn timeseries_table() -> TimeSeriesTable {
    TimeSeriesTable {
        schema: Some("my_nas".to_string()),
        time_series_table: "ts.parquet".to_string(),
        value_column: "v".to_string(),
        timestamp_column: "ts".to_string(),
        identifier_column: "id".to_string(),
        value_datatype: xsd::UNSIGNED_INT.into_owned(),
        year_column: None,
        month_column: None,
        day_column: None,
    }
}

async fn ts_sql_db(timeseries_table: TimeSeriesTable) -> ArrowFlightSQLDatabase {
    ArrowFlightSQLDatabase::new(
        ARROW_SQL_DATABASE_ENDPOINT,
        "dremio",
        "dremio123",
        vec![timeseries_table],
    )
    .await
    .unwrap()
}

#[derive(Deserialize)]
struct Token {
    pub token: String,
}

#[derive(Serialize)]
#[allow(non_snake_case)]
struct UserPass {
    pub userName: String,
    pub password: String,
}

#[derive(Serialize)]
struct NasConfig {
    pub path: String,
}

#[fixture]
async fn with_timeseries_testdata(#[future] arrow_sql_endpoint: ()) {
    let _ = arrow_sql_endpoint.await;
    let c = reqwest::Client::new();
    let mut bld = c.request(Method::POST, format!("{}/apiv2/login", DREMIO_ORIGIN));
    let user_pass = UserPass {
        userName: "dremio".to_string(),
        password: "dremio123".to_string(),
    };
    bld = bld.header(CONTENT_TYPE, "application/json");
    bld = bld.json(&user_pass);
    let res = bld.send().await.unwrap();
    let token = res.json::<Token>().await.unwrap();
    //println!("Token: {}", token.token);

    //Add source
    let mut bld = c.request(Method::POST, format!("{}/api/v3/catalog", DREMIO_ORIGIN));
    bld = bld.bearer_auth(token.token.clone());
    let create = r#"
    {
  "entityType": "source",
  "config": {
    "path": "/var/dremio-data"
  },
  "type": "NAS",
  "name": "my_nas",
  "metadataPolicy": {
    "authTTLMs": 86400000,
    "namesRefreshMs": 3600000,
    "datasetRefreshAfterMs": 3600000,
    "datasetExpireAfterMs": 10800000,
    "datasetUpdateMode": "PREFETCH_QUERIED",
    "deleteUnavailableDatasets": true,
    "autoPromoteDatasets": false
  },
  "accelerationGracePeriodMs": 10800000,
  "accelerationRefreshPeriodMs": 3600000,
  "accelerationNeverExpire": false,
  "accelerationNeverRefresh": false
}
    "#;
    bld = bld.body(create);
    bld = bld.header(CONTENT_TYPE, "application/json");
    let resp = bld.send().await.unwrap().text().await.unwrap();
    println!("Resp {:?}", resp);

    //Promote file in source
    let mut bld = c.request(
        Method::POST,
        format!(
            "{}/api/v3/catalog/dremio%3A%2Fmy_nas%2Fts.parquet",
            DREMIO_ORIGIN
        ),
    );
    bld = bld.bearer_auth(token.token.clone());
    let create = r#"
    {
  "entityType": "dataset",
    "id": "dremio:/my_nas/ts.parquet",
    "path": [
    	"my_nas", "ts.parquet"
    	],

    "type": "PHYSICAL_DATASET",
    "format": {
        "type": "Parquet"
    }
}
    "#;
    bld = bld.body(create);
    bld = bld.header(CONTENT_TYPE, "application/json");
    let resp = bld.send().await.unwrap().text().await.unwrap();
    println!("Resp {:?}", resp);
}

#[rstest]
#[tokio::test]
#[serial]
async fn test_simple_hybrid_query(
    #[future] with_sparql_testdata: (),
    #[future] with_timeseries_testdata: (),
    timeseries_table: TimeSeriesTable,
    shared_testdata_path: PathBuf,
    use_logger: (),
) {
    let _ = use_logger;
    let _ = with_sparql_testdata.await;
    let _ = with_timeseries_testdata.await;
    let db = ts_sql_db(timeseries_table).await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/magbak/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    "#;
    let mut engine = Engine::new(all_pushdowns(), Box::new(db), QUERY_ENDPOINT.to_string());
    let mut df = engine
        .execute_hybrid_query(query)
        .await
        .expect("Hybrid error");
    df.with_column(
        df.column("t")
            .unwrap()
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap(),
    )
    .unwrap();
    let mut file_path = shared_testdata_path.clone();
    file_path.push("expected_simple_hybrid.csv");

    let file = File::open(file_path.as_path()).expect("Read file problem");
    let expected_df = CsvReader::new(file)
        .infer_schema(None)
        .has_header(true)
        .with_try_parse_dates(true)
        .finish()
        .expect("DF read error");
    assert_eq!(expected_df, df);
}
