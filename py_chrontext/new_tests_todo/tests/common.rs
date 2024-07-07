use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::models::{ContainerSummary, HostConfig, PortBinding};
use bollard::Docker;
use polars::prelude::{CsvParseOptions, CsvReadOptions, DataFrame, SerReader};
use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

const OXIGRAPH_SERVER_IMAGE: &str = "oxigraph/oxigraph:v0.3.8";
const UPDATE_ENDPOINT: &str = "http://localhost:7878/update";

pub const QUERY_ENDPOINT: &str = "http://localhost:7878/query";

pub async fn find_container(docker: &Docker, container_name: &str) -> Option<ContainerSummary> {
    let list = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: true,
            ..Default::default()
        }))
        .await
        .expect("List containers problem");
    let slashed_container_name = "/".to_string() + container_name;
    let existing = list
        .iter()
        .find(|cs| {
            cs.names.is_some()
                && cs
                    .names
                    .as_ref()
                    .unwrap()
                    .iter()
                    .any(|n| &n == &&slashed_container_name)
        })
        .cloned();
    existing
}

pub async fn start_sparql_container() {
    let docker = Docker::connect_with_local_defaults().expect("Could not find local docker");
    let container_name = "my-oxigraph-server";
    let existing = find_container(&docker, container_name).await;
    if let Some(existing) = existing {
        if let Some(state) = &existing.state {
            println!("Existing container state: {}", state);
            if state == "running" {
                return;
            }
        }
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
    let options = CreateContainerOptions {
        name: container_name,
        platform: None,
    };
    let config = Config {
        image: Some(OXIGRAPH_SERVER_IMAGE),
        cmd: Some(vec![
            "--location",
            "/data",
            "serve",
            "--bind",
            "0.0.0.0:7878",
        ]),
        exposed_ports: Some(HashMap::from([("7878/tcp", HashMap::new())])),
        host_config: Some(HostConfig {
            port_bindings: Some(HashMap::from([(
                "7878/tcp".to_string(),
                Some(vec![PortBinding {
                    host_ip: None,
                    host_port: Some("7878/tcp".to_string()),
                }]),
            )])),
            ..Default::default()
        }),
        ..Default::default()
    };
    docker
        .create_container(Some(options), config)
        .await
        .expect("Problem creating container");
    docker
        .start_container(container_name, None::<StartContainerOptions<String>>)
        .await
        .expect("Started container problem ");
    sleep(Duration::from_secs(10)).await;
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

pub async fn wipe_database() {
    let delete_all_query = r#"
    DELETE {?s ?v ?o } WHERE {?s ?v ?o}
    "#;
    let client = reqwest::Client::new();
    let put_request = client
        .post(UPDATE_ENDPOINT)
        .header(CONTENT_TYPE, "application/sparql-update")
        .body(delete_all_query);
    let put_response = put_request.send().await.expect("Update error");
    assert_eq!(put_response.status(), StatusCode::from_u16(204).unwrap());
}

pub async fn add_sparql_testdata(testdata_path: PathBuf) {
    let testdata_update_string =
        fs::read_to_string(testdata_path.as_path()).expect("Read testdata.sparql problem");

    let client = reqwest::Client::new();
    let put_request = client
        .post(UPDATE_ENDPOINT)
        .header(CONTENT_TYPE, "application/sparql-update")
        .body(testdata_update_string);
    let put_response = put_request.send().await.expect("Update error");
    assert_eq!(put_response.status(), StatusCode::from_u16(204).unwrap());
}

pub fn read_csv(file_path: PathBuf) -> DataFrame {
    let opts = CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_try_parse_dates(true));
    let df = opts
        .try_into_reader_with_file_path(Some(file_path))
        .unwrap()
        .finish()
        .unwrap();
    df
}
