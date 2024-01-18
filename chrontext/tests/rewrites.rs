use chrontext::preprocessing::Preprocessor;
use representation::query_context::{Context, PathEntry, VariableInContext};
use chrontext::rewriting::StaticQueryRewriter;
use chrontext::splitter::parse_sparql_select_query;
use chrontext::timeseries_query::BasicTimeseriesQuery;
use spargebra::term::Variable;
use spargebra::Query;

#[test]
fn test_simple_query() {
    let sparql = r#"
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
     ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
     ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_filtered_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val > 0.5 && ?t >= "2016-01-01"^^xsd:dateTime)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_external_id_0 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
     ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
     ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
      }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_complex_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 ex:hasPropertyValue ?pv .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val > 0.5 && ?t >= "2016-01-01"^^xsd:dateTime && ?pv)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_external_id_0 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
    ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
    FILTER(?pv) }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_complex_expression_filter_projection() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 ex:hasPropertyValue ?pv .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val > ?pv || ?t >= "2016-01-01"^^xsd:dateTime)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_external_id_0 ?pv WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
    ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_complex_nested_expression_filter() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 WHERE {
        ?var1 a ?var2 .
        ?var2 ex:hasPropertyValue ?pv .
        ?var2 qry:hasTimeseries ?ts .
        ?ts qry:hasDataPoint ?dp .
        ?dp qry:hasValue ?val .
        ?dp qry:hasTimestamp ?t .
        FILTER(?val <= 0.5 || !(?pv))
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_external_id_0 ?pv WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var2 <https://example.com/hasPropertyValue> ?pv .
    ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
    ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
     }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_option_expression_filter_projection() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?pv ?t ?val WHERE {
        ?var1 a ?var2 .
        OPTIONAL {
            ?var2 ex:hasPropertyValue ?pv .
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val <= 0.5 && !(?pv))
        }
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 2);
    let static_rewrite_left = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::LeftJoinLeftSide,
        ]))
        .unwrap();
    let static_rewrite_right = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::LeftJoinRightSide,
        ]))
        .unwrap();

    let expected_left_str = r#"SELECT ?var1 ?var2 WHERE { ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 . }"#;
    let expected_right_str = r#"SELECT ?pv ?ts ?ts_external_id_0 ?ts_resource_0 ?var2 WHERE { ?var2 <https://example.com/hasPropertyValue> ?pv .?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 . }"#;

    let expected_left_query = Query::parse(expected_left_str, None).unwrap();
    assert_eq!(static_rewrite_left, &expected_left_query);
    let expected_right_query = Query::parse(expected_right_str, None).unwrap();
    assert_eq!(static_rewrite_right, &expected_right_query);
}

#[test]
fn test_union_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?pv WHERE {
        ?var1 a ?var2 .
        OPTIONAL {
            {
            ?var2 ex:hasPropertyValue ?pv .
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val <= 0.5 && !(?pv))
            } UNION {
            ?var2 ex:hasPropertyValue ?pv .
            ?var2 qry:hasTimeseries ?ts .
            ?ts qry:hasDataPoint ?dp .
            ?dp qry:hasValue ?val .
            ?dp qry:hasTimestamp ?t .
            FILTER(?val > 100.0 && ?pv)
            }
            }
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 3);

    let static_union_left_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::LeftJoinRightSide,
            PathEntry::UnionLeftSide,
        ]))
        .unwrap();
    let expected_union_left_str = r#"SELECT ?pv ?ts ?ts_external_id_0 ?ts_resource_0 ?var2 WHERE { ?var2 <https://example.com/hasPropertyValue> ?pv .?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 . ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 . FILTER(!?pv) }"#;
    let expected_union_left_query = Query::parse(expected_union_left_str, None).unwrap();
    assert_eq!(static_union_left_rewrite, &expected_union_left_query);

    let static_union_right_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::LeftJoinRightSide,
            PathEntry::UnionRightSide,
        ]))
        .unwrap();
    let expected_union_right_str = r#"SELECT ?pv ?ts ?ts_external_id_1 ?ts_resource_1 ?var2 WHERE { ?var2 <https://example.com/hasPropertyValue> ?pv .?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_1 . ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_1 . FILTER(?pv) }"#;
    let expected_union_right_query = Query::parse(expected_union_right_str, None).unwrap();
    assert_eq!(static_union_right_rewrite, &expected_union_right_query);

    let static_leftjoin_left_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::LeftJoinLeftSide,
        ]))
        .unwrap();
    let expected_leftjoin_left_str = r#"SELECT ?var1 ?var2 WHERE { ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 . }"#;
    let expected_leftjoin_left_query = Query::parse(expected_leftjoin_left_str, None).unwrap();
    assert_eq!(static_leftjoin_left_rewrite, &expected_leftjoin_left_query);
}

#[test]
fn test_bind_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?val3 WHERE {
        ?var1 a ?var2 .
        ?var1 qry:hasTimeseries ?ts1 .
        ?ts1 qry:hasDataPoint ?dp1 .
        ?dp1 qry:hasValue ?val1 .
        ?dp1 qry:hasTimestamp ?t .
        ?var2 qry:hasTimeseries ?ts2 .
        ?ts2 qry:hasDataPoint ?dp2 .
        ?dp2 qry:hasValue ?val2 .
        ?dp2 qry:hasTimestamp ?t .
        BIND((?val1 + ?val2) as ?val3)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_resource_1 ?ts_external_id_0 ?ts_external_id_1 WHERE {
    ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
    ?var1 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts1 .
    ?ts1 <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
    ?ts1 <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
    ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts2 .
    ?ts2 <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_1 .
    ?ts2 <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_1 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_fix_dropped_triple() {
    let sparql = r#"
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
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime && ?v < 50) .
    }"#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, time_series_queries, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w ?s ?ts_resource_0 ?ts_external_id_0 WHERE {
        ?w a types:BigWidget .
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasExternalId ?ts_external_id_0 .
        ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
    }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);

    let expected_time_series_queries = vec![BasicTimeseriesQuery {
        identifier_variable: Some(Variable::new_unchecked("ts_external_id_0")),
        timeseries_variable: Some(VariableInContext::new(
            Variable::new_unchecked("ts"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        data_point_variable: Some(VariableInContext::new(
            Variable::new_unchecked("dp"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        value_variable: Some(VariableInContext::new(
            Variable::new_unchecked("v"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        resource_variable: Some(Variable::new_unchecked("ts_resource_0")),
        resource: None,
        timestamp_variable: Some(VariableInContext::new(
            Variable::new_unchecked("t"),
            Context::from_path(vec![
                PathEntry::ProjectInner,
                PathEntry::FilterInner,
                PathEntry::BGP,
            ]),
        )),
        ids: None,
    }];
    assert_eq!(time_series_queries, expected_time_series_queries);
}

#[test]
fn test_property_path_expression() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX qry:<https://github.com/DataTreehouse/chrontext#>
    PREFIX ex:<https://example.com/>
    SELECT ?var1 ?var2 ?val3 WHERE {
        ?var1 a ?var2 .
        ?var1 qry:hasTimeseries / qry:hasDataPoint ?dp1 .
        ?dp1 qry:hasValue ?val1 .
        ?dp1 qry:hasTimestamp ?t .
        ?var2 qry:hasTimeseries / qry:hasDataPoint ?dp2 .
        ?dp2 qry:hasValue ?val2 .
        ?dp2 qry:hasTimestamp ?t .
        BIND((?val1 + ?val2) as ?val3)
        }
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, time_series_queries, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();

    let expected_str = r#"
    SELECT ?var1 ?var2 ?ts_resource_0 ?ts_resource_1 ?ts_external_id_0 ?ts_external_id_1 WHERE {
     ?var1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?var2 .
     ?var1 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?blank_replacement_0 .
     ?blank_replacement_0 <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
     ?blank_replacement_0 <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
     ?var2 <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?blank_replacement_1 .
     ?blank_replacement_1 <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_1 .
     ?blank_replacement_1 <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_1 . }
    "#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    let expected_time_series_queries = vec![
        BasicTimeseriesQuery {
            identifier_variable: Some(Variable::new_unchecked("ts_external_id_0")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("blank_replacement_0"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("val1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            resource_variable: Some(Variable::new_unchecked("ts_resource_0")),
            resource: None,
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            ids: None,
        },
        BasicTimeseriesQuery {
            identifier_variable: Some(Variable::new_unchecked("ts_external_id_1")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("blank_replacement_1"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp2"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("val2"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            resource_variable: Some(Variable::new_unchecked("ts_resource_1")),
            resource: None,
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::from_path(vec![
                    PathEntry::ProjectInner,
                    PathEntry::ExtendInner,
                    PathEntry::BGP,
                ]),
            )),
            ids: None,
        },
    ];
    assert_eq!(time_series_queries, expected_time_series_queries);
    assert_eq!(static_rewrite, &expected_query);
}

#[test]
fn test_having_query() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX chrontext:<https://github.com/DataTreehouse/chrontext#>
    PREFIX types:<http://example.org/types#>
    SELECT ?w (SUM(?v) as ?sum_v) WHERE {
        ?w types:hasSensor ?s .
        ?s chrontext:hasTimeseries ?ts .
        ?ts chrontext:hasDataPoint ?dp .
        ?dp chrontext:hasTimestamp ?t .
        ?dp chrontext:hasValue ?v .
        BIND(FLOOR(seconds(?t) / 5.0) as ?second_5)
        BIND(minutes(?t) AS ?minute)
        BIND(hours(?t) AS ?hour)
        BIND(day(?t) AS ?day)
        BIND(month(?t) AS ?month)
        BIND(year(?t) AS ?year)
        FILTER(?t > "2022-06-01T08:46:53"^^xsd:dateTime)
    } GROUP BY ?w ?year ?month ?day ?hour ?minute ?second_5
    HAVING (SUM(?v) > 1000)
    "#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_groupby_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::ExtendInner,
            PathEntry::FilterInner,
            PathEntry::GroupInner,
        ]))
        .unwrap();
    let expected_groupby_str = r#"SELECT ?s ?ts ?ts_external_id_0 ?ts_resource_0 ?w WHERE { ?w <http://example.org/types#hasSensor> ?s .?s <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 . ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 . }"#;
    let expected_groupby_query = Query::parse(expected_groupby_str, None).unwrap();
    assert_eq!(static_groupby_rewrite, &expected_groupby_query);
    //println!("{}", static_rewrite);
}

#[test]
fn test_exists_query() {
    let sparql = r#"
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
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 2);
    let static_inner_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::FilterInner,
        ]))
        .unwrap();
    let expected_inner_str =
        r#"SELECT ?s ?w WHERE { ?w <http://example.org/types#hasSensor> ?s . }"#;
    let expected_inner_query = Query::parse(expected_inner_str, None).unwrap();
    assert_eq!(static_inner_rewrite, &expected_inner_query);

    let static_expr_rewrite = static_rewrites_map
        .get(&Context::from_path(vec![
            PathEntry::ProjectInner,
            PathEntry::FilterExpression,
            PathEntry::Exists,
        ]))
        .unwrap();
    let expected_expr_str = r#"SELECT ?s ?ts_resource_0 ?ts_external_id_0 WHERE { ?s <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 . ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 . }"#;
    let expected_expr_query = Query::parse(expected_expr_str, None).unwrap();
    assert_eq!(static_expr_rewrite, &expected_expr_query);
    //println!("{}", static_rewrite);
}

#[test]
fn test_filter_lost_bug() {
    let sparql = r#"
    PREFIX xsd:<http://www.w3.org/2001/XMLSchema#>
    PREFIX ct:<https://github.com/DataTreehouse/chrontext#>
    PREFIX wp:<https://github.com/DataTreehouse/chrontext/windpower_example#>
    PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rds:<https://github.com/DataTreehouse/chrontext/rds_power#>
    SELECT ?site_label ?wtur_label ?ts ?val ?t WHERE {
    ?site a rds:Site .
    ?site rdfs:label ?site_label .
    ?site rds:hasFunctionalAspect ?wtur_asp .
    ?wtur_asp rdfs:label ?wtur_label .
    ?wtur rds:hasFunctionalAspectNode ?wtur_asp .
    ?wtur rds:hasFunctionalAspect ?gensys_asp .
    ?gensys rds:hasFunctionalAspectNode ?gensys_asp .
    ?gensys ct:hasTimeseries ?ts .
    ?ts rdfs:label "Production" .
    ?ts ct:hasDataPoint ?dp .
    ?dp ct:hasValue ?val .
    ?dp ct:hasTimestamp ?t .
    FILTER(?wtur_label = "A1" && ?t > "2022-06-17T08:46:53"^^xsd:dateTime) .
}"#;
    let parsed = parse_sparql_select_query(sparql).unwrap();
    let mut preprocessor = Preprocessor::new();
    let (preprocessed_query, has_constraint) = preprocessor.preprocess(&parsed);
    let rewriter = StaticQueryRewriter::new(&has_constraint);
    let (static_rewrites_map, _, _) = rewriter.rewrite_query(preprocessed_query);
    assert_eq!(static_rewrites_map.len(), 1);
    let static_rewrite = static_rewrites_map.get(&Context::new()).unwrap();
    let expected_str = r#"
    SELECT ?site_label ?wtur_label ?ts ?ts_resource_0 ?ts_external_id_0 WHERE {
    ?site <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://github.com/DataTreehouse/chrontext/rds_power#Site> .
    ?site <http://www.w3.org/2000/01/rdf-schema#label> ?site_label .
    ?site <https://github.com/DataTreehouse/chrontext/rds_power#hasFunctionalAspect> ?wtur_asp .
    ?wtur_asp <http://www.w3.org/2000/01/rdf-schema#label> ?wtur_label .
    ?wtur <https://github.com/DataTreehouse/chrontext/rds_power#hasFunctionalAspectNode> ?wtur_asp .
    ?wtur <https://github.com/DataTreehouse/chrontext/rds_power#hasFunctionalAspect> ?gensys_asp .
    ?gensys <https://github.com/DataTreehouse/chrontext/rds_power#hasFunctionalAspectNode> ?gensys_asp .
    ?gensys <https://github.com/DataTreehouse/chrontext#hasTimeseries> ?ts .
    ?ts <http://www.w3.org/2000/01/rdf-schema#label> "Production" .
    ?ts <https://github.com/DataTreehouse/chrontext#hasExternalId> ?ts_external_id_0 .
    ?ts <https://github.com/DataTreehouse/chrontext#hasResource> ?ts_resource_0 .
    FILTER((?wtur_label = "A1"))
    }"#;
    let expected_query = Query::parse(expected_str, None).unwrap();
    assert_eq!(static_rewrite, &expected_query);
}
