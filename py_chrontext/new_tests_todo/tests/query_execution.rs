mod common;
mod timeseries_in_memory_database;

use chrontext::engine::Engine;
use chrontext::sparql_database::sparql_endpoint::SparqlEndpoint;
use log::debug;
use polars::prelude::{col, DataType, IntoLazy, SortMultipleOptions};
use rstest::*;
use serial_test::serial;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use virtualized_query::pushdown_setting::all_pushdowns;

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
    testdata_path.push("query_execution");
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
fn inmem_virtualized_database(testdata_path: PathBuf) -> TimeseriesInMemoryDatabase {
    let mut frames = HashMap::new();
    for t in ["ts1", "ts2"] {
        let mut file_path = testdata_path.clone();
        file_path.push(t.to_string() + ".csv");
        //Important to cast to int32 for equality to work as datatypes must match exactly.
        let df = read_csv(file_path)
            .lazy()
            .with_column(col("value").cast(DataType::Int32))
            .collect()
            .unwrap();
        frames.insert(t.to_string(), df);
    }
    TimeseriesInMemoryDatabase { frames }
}

#[fixture]
fn engine(inmem_virtualized_database: TimeseriesInMemoryDatabase) -> Engine {
    Engine::new(
        all_pushdowns(),
        Arc::new(inmem_virtualized_database),
        Arc::new(SparqlEndpoint {
            endpoint: QUERY_ENDPOINT.to_string(),
        }),
    )
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_simple_hybrid_query(
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
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_simple_hybrid.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_simple_hybrid_no_vq_matches_query(
    #[future] with_testdata: (),
    engine: Engine,
    use_logger: (),
) {
    use_logger;
    let _ = with_testdata.await;
    let query = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?t ?v WHERE {
        ?w a types:BigWidgetInvalidType .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 200) .
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;

    assert_eq!(df.height(), 0);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_complex_hybrid_query(
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
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_complex_hybrid.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_group_by_hybrid_query(
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
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_group_by_second_hybrid_query(
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
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(seconds(?t) as ?second)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "sum_v"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_second_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "sum_v"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_group_by_second_having_hybrid_query(
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
    SELECT ?w (CONCAT(?year, "-", ?month, "-", ?day, "-", ?hour, "-", ?minute, "-", (?second_5*5)) as ?period) (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 5.0)) as ?second_5)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second_5
    HAVING (SUM(?v)>100)
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "sum_v"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_second_having_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "sum_v"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    //println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_union_of_two_groupby_queries(
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
SELECT ?w ?second_5 ?kind ?sum_v WHERE {
  {
    SELECT ?w ?kind ?second_5 (SUM(?v) AS ?sum_v) WHERE {
      ?w types:hasSensor ?s.
      ?s chrontext:hasTimeseries ?ts.
      ?ts chrontext:hasDataPoint ?dp.
      ?dp chrontext:hasTimestamp ?t;
        chrontext:hasValue ?v.
      BIND("under_500" AS ?kind)
      BIND(xsd:integer(FLOOR((SECONDS(?t)) / "5.0"^^xsd:decimal)) AS ?second_5)
      BIND(MINUTES(?t) AS ?minute)
      FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    }
    GROUP BY ?w ?kind ?minute ?second_5
    HAVING ((SUM(?v)) < 500 )
  }
  UNION
  {
    SELECT ?w ?kind ?second_5 (SUM(?v) AS ?sum_v) WHERE {
      ?w types:hasSensor ?s.
      ?s chrontext:hasTimeseries ?ts.
      ?ts chrontext:hasDataPoint ?dp.
      ?dp chrontext:hasTimestamp ?t;
        chrontext:hasValue ?v.
      BIND("over_1000" AS ?kind)
      BIND(xsd:integer(FLOOR((SECONDS(?t)) / "5.0"^^xsd:decimal)) AS ?second_5)
      BIND(MINUTES(?t) AS ?minute)
      FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    }
    GROUP BY ?w ?kind ?minute ?second_5
    HAVING ((SUM(?v)) > 1000 )
  }
}
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .lazy()
        .with_columns([
            col("w").cast(DataType::String),
            col("kind").cast(DataType::String),
        ])
        .sort_by_exprs(
            [col("w"), col("kind"), col("second_5")],
            SortMultipleOptions::default(),
        )
        .collect()
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_union_of_two_groupby.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "kind", "second_5"], SortMultipleOptions::default())
        .expect("Sort error");
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
async fn test_pushdown_group_by_concat_agg_hybrid_query(
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
    SELECT ?w ?seconds_5 (GROUP_CONCAT(?v ; separator="-") as ?cc) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 5.0)) as ?seconds_5)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?seconds_5
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "seconds_5"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_concat_agg_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "seconds_5"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    //println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_groupby_exists_something_hybrid_query(
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
    SELECT ?w ?seconds_3 (AVG(?v) as ?mean) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(xsd:integer(FLOOR(seconds(?t) / 3.0)) as ?seconds_3)
        FILTER EXISTS {SELECT ?w WHERE {?w types:hasSomething ?smth}}
    } GROUP BY ?w ?seconds_3
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "seconds_3"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_group_by_exists_something_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "seconds_3"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_groupby_exists_timeseries_value_hybrid_query(
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
        ?w types:hasSensor ?s .
        FILTER EXISTS {SELECT ?s WHERE {
            ?s chrontext:hasTimeseries ?ts .
            ?ts chrontext:hasDataPoint ?dp .
            ?dp chrontext:hasTimestamp ?t .
            ?dp chrontext:hasValue ?v .
            FILTER(?v > 300)}}
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_exists_timeseries_value_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_groupby_exists_aggregated_timeseries_value_hybrid_query(
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
        ?w types:hasSensor ?s .
        FILTER EXISTS {SELECT ?s WHERE {
            ?s chrontext:hasTimeseries ?ts .
            ?ts chrontext:hasDataPoint ?dp .
            ?dp chrontext:hasTimestamp ?t .
            ?dp chrontext:hasValue ?v .
            FILTER(?v < 300)}
            GROUP BY ?s
            HAVING (SUM(?v) >= 1000)
            }
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_exists_aggregated_timeseries_value_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_pushdown_groupby_not_exists_aggregated_timeseries_value_hybrid_query(
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
        ?w types:hasSensor ?s .
        FILTER NOT EXISTS {SELECT ?s WHERE {
            ?s chrontext:hasTimeseries ?ts .
            ?ts chrontext:hasDataPoint ?dp .
            ?dp chrontext:hasTimestamp ?t .
            ?dp chrontext:hasValue ?v .
            FILTER(?v < 300)}
            GROUP BY ?s
            HAVING (SUM(?v) >= 1000)
            }
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_pushdown_not_exists_aggregated_timeseries_value_hybrid.csv");

    let expected_df = read_csv(file_path)
        .sort(["w"], SortMultipleOptions::default())
        .expect("Sort error");
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
async fn test_path_group_by_query(
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
    SELECT ?w (MAX(?v) as ?max_v) WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint/chrontext:hasValue ?v .}
        GROUP BY ?w
        ORDER BY ASC(?max_v)
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_path_group_by_query.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_optional_clause_query(
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
    SELECT ?w ?v ?greater WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        OPTIONAL {
        BIND(?v>300 as ?greater)
        FILTER(?greater)
        }
    }
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .lazy()
        .with_column(col("w").cast(DataType::String))
        .collect()
        .unwrap();

    df = df
        .sort(["w", "v", "greater"], SortMultipleOptions::default())
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_optional_clause_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "v", "greater"], SortMultipleOptions::default())
        .unwrap();
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
async fn test_minus_query(
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
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        MINUS {
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        FILTER(?v > 300)
        }
    }
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .lazy()
        .with_column(col("w").cast(DataType::String))
        .collect()
        .unwrap();
    df = df
        .sort(["w", "v"], SortMultipleOptions::default())
        .expect("Sort error");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_minus_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "v"], SortMultipleOptions::default())
        .expect("Sort error");

    assert_eq!(expected_df, df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_in_expression_query(
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
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        FILTER(?v IN (("300"^^xsd:int + "4"^^xsd:int), ("304"^^xsd:int - "3"^^xsd:int), "307"^^xsd:int))
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    let mut file_path = testdata_path.clone();
    file_path.push("expected_in_expression.csv");

    let expected_df = read_csv(file_path);
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_values_query(
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
    SELECT ?w ?v WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        VALUES ?v2 { 301 304 307 }
        FILTER(xsd:integer(?v) = xsd:integer(?v2))
    }
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .sort(&["v", "w"], SortMultipleOptions::default())
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_values_query.csv");

    let mut expected_df = read_csv(file_path);
    expected_df = expected_df
        .sort(&["v", "w"], SortMultipleOptions::default())
        .unwrap();

    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_if_query(
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
    SELECT ?w (IF(?v>300,?v,300) as ?v_with_min) WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "v_with_min"], SortMultipleOptions::default())
        .expect("Sort problem");
    let mut file_path = testdata_path.clone();
    file_path.push("expected_if_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "v_with_min"], SortMultipleOptions::default())
        .expect("Sort problem");

    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_distinct_query(
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
    SELECT DISTINCT ?w (IF(?v>300,?v,300) as ?v_with_min) WHERE {
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
    }
    "#;
    let df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0
        .sort(["w", "v_with_min"], SortMultipleOptions::default())
        .unwrap();
    let mut file_path = testdata_path.clone();
    file_path.push("expected_distinct_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "v_with_min"], SortMultipleOptions::default())
        .unwrap();
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_union_query(
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
    SELECT ?w ?v WHERE {
        { ?w a types:BigWidget .
        ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasValue ?v .
        FILTER(?v > 100) }
        UNION {
            ?w a types:SmallWidget .
            ?w types:hasSensor/chrontext:hasTimeseries/chrontext:hasDataPoint ?dp .
            ?dp chrontext:hasValue ?v .
            FILTER(?v < 100)
        }
    }
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .lazy()
        .with_columns([col("w").cast(DataType::String)])
        .sort_by_exprs([col("w"), col("v")], SortMultipleOptions::default())
        .collect()
        .unwrap();

    let mut file_path = testdata_path.clone();
    file_path.push("expected_union_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["w", "v"], SortMultipleOptions::default())
        .expect("Sort problem");

    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}

#[rstest]
#[tokio::test]
#[serial]
#[allow(path_statements)]
async fn test_coalesce_query(
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
    SELECT ?s1 ?t1 ?v1 ?v2 (COALESCE(?v2, ?v1) as ?c) WHERE {
        ?s1 chrontext:hasTimeseries/chrontext:hasDataPoint ?dp1 .
        ?dp1 chrontext:hasValue ?v1 .
        ?dp1 chrontext:hasTimestamp ?t1 .
        OPTIONAL {
        ?s1 chrontext:hasTimeseries/chrontext:hasDataPoint ?dp2 .
        ?dp2 chrontext:hasValue ?v2 .
        ?dp2 chrontext:hasTimestamp ?t2 .
        FILTER(seconds(?t2) >= (seconds(?t1) - 1) && seconds(?t2) <= (seconds(?t1) + 1) && ?v2 > ?v1)
        }
    }
    "#;
    let mut df = engine
        .query(query)
        .await
        .expect("Hybrid error")
        .0;
    df = df
        .lazy()
        .with_column(col("s1").cast(DataType::String))
        .collect()
        .unwrap();
    df = df
        .sort(["s1", "t1", "v1", "v2"], SortMultipleOptions::default())
        .expect("Sort problem");

    let mut file_path = testdata_path.clone();
    file_path.push("expected_coalesce_query.csv");

    let expected_df = read_csv(file_path)
        .sort(["s1", "t1", "v1", "v2"], SortMultipleOptions::default())
        .expect("Sort problem");
    assert_eq!(expected_df, df);
    // let file = File::create(file_path.as_path()).expect("could not open file");
    // let mut writer = CsvWriter::new(file);
    // writer.finish(&mut df).expect("writeok");
    // println!("{}", df);
}
