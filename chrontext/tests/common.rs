use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::models::{ContainerSummary, HostConfig, PortBinding};
use bollard::Docker;
use oxrdf::Term;
use reqwest::header::CONTENT_TYPE;
use reqwest::StatusCode;
use sparesults::QuerySolution;
use std::cmp::Ordering;
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

#[allow(dead_code)]
pub fn compare_terms(t1: &Term, t2: &Term) -> Ordering {
    let t1_string = t1.to_string();
    let t2_string = t2.to_string();
    t1_string.cmp(&t2_string)
}

#[allow(dead_code)]
pub fn compare_query_solutions(a: &QuerySolution, b: &QuerySolution) -> Ordering {
    let mut first_unequal = None;
    for (av, at) in a {
        if let Some(bt) = b.get(av) {
            let comparison = compare_terms(at, bt);
            if Ordering::Equal != comparison {
                first_unequal = Some(comparison);
                break;
            }
        } else {
            first_unequal = Some(Ordering::Greater);
            break;
        }
    }
    if let Some(ordering) = first_unequal {
        return ordering;
    }
    for (bv, _) in b {
        if a.get(bv).is_none() {
            return Ordering::Less;
        }
    }
    Ordering::Equal
}

#[allow(dead_code)]
pub fn compare_all_solutions(mut expected: Vec<QuerySolution>, mut actual: Vec<QuerySolution>) {
    assert_eq!(expected.len(), actual.len());
    expected.sort_by(compare_query_solutions);
    actual.sort_by(compare_query_solutions);
    let mut i = 0;
    for es in &expected {
        assert_eq!(
            compare_query_solutions(es, actual.get(i).unwrap()),
            Ordering::Equal
        );
        i += 1;
    }
}
