use std::sync::Arc;

use axum::response::Html;
use axum::Form;
use axum::{self, Router};
use axum::{extract::State, routing::get};

use oxigraph::sparql::{results::QueryResultsFormat, QueryResults, QuerySolutionIter};
use oxrdf::Variable;
use serde::Deserialize;

use crate::sparql_database::SparqlQueryable;

#[derive(Clone)]
struct AppState {
    sparql_engine: Arc<(dyn SparqlQueryable)>,
}

pub async fn launch_web(sparql_engine: Arc<(dyn SparqlQueryable)>, address: &str) {
    let state = AppState { sparql_engine };

    let app: Router = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/query", get(get_query).post(post_query))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_query() -> Html<&'static str> {
    Html(include_str!("web/sparql.html"))
}

#[derive(Deserialize, Debug)]
struct SparqlQuery {
    query: String,
}

async fn post_query(State(state): State<AppState>, Form(form): Form<SparqlQuery>) -> String {
    let sparql_engine = state.sparql_engine;

    let query = spargebra::Query::parse(&form.query, None).unwrap();

    let query_result: QueryResults = match sparql_engine.execute(&query).await {
        Ok(v) => {
            let variables: Arc<[Variable]> = v.first().unwrap().variables().into();
            let iter = v
                .into_iter()
                .map(|qs| Ok(qs.values().iter().map(|t| t.clone()).collect()));
            let qsi = QuerySolutionIter::new(variables, iter);
            qsi.into()
        }
        Err(_) => todo!(),
    };

    let mut results = Vec::new();
    query_result.write(&mut results, QueryResultsFormat::Json);

    String::from_utf8_lossy(&results).into()
}
