use crate::combiner::Combiner;
use crate::constants::GROUPING_COL;
use crate::pushdown_setting::all_pushdowns;
use crate::sparql_database::sparql_endpoint::SparqlEndpoint;
use crate::timeseries_database::{DatabaseType, get_datatype_map, TimeseriesQueryable};
use crate::timeseries_query::{
    BasicTimeseriesQuery, GroupedTimeseriesQuery, Synchronizer, TimeseriesQuery,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use polars::frame::DataFrame;
use polars::prelude::DataFrameJoinOps;
use polars::prelude::{col, concat, lit, IntoLazy, JoinArgs, JoinType, UnionArgs};
use query_processing::aggregates::AggregateReturn;
use query_processing::graph_patterns::extend;
use representation::query_context::{Context, PathEntry};
use representation::solution_mapping::SolutionMappings;
use representation::{RDFNodeType};
use spargebra::algebra::Expression;
use std::collections::HashMap;
use std::error::Error;

pub struct TimeseriesInMemoryDatabase {
    pub frames: HashMap<String, DataFrame>,
}

#[async_trait]
impl TimeseriesQueryable for TimeseriesInMemoryDatabase {
    fn get_database_type(&self) -> DatabaseType {
        DatabaseType::InMemory
    }

    async fn execute(&mut self, tsq: &TimeseriesQuery) -> Result<SolutionMappings, Box<dyn Error>> {
        self.execute_query(tsq).await
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl TimeseriesInMemoryDatabase {
    #[async_recursion]
    async fn execute_query(&self, tsq: &TimeseriesQuery) -> Result<SolutionMappings, Box<dyn Error>> {
        let (df, dtypes) = self.execute_query_impl(tsq).await?;
        Ok(SolutionMappings::new(df.lazy(), dtypes))
    }
    #[async_recursion]
    async fn execute_query_impl(
        &self,
        tsq: &TimeseriesQuery,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        match tsq {
            TimeseriesQuery::Basic(b) => self.execute_basic(b),
            TimeseriesQuery::Filtered(inner, filter) => self.execute_filtered(inner, filter).await,
            TimeseriesQuery::InnerSynchronized(inners, synchronizers) => {
                self.execute_inner_synchronized(inners, synchronizers).await
            }
            TimeseriesQuery::Grouped(grouped) => self.execute_grouped(grouped).await,
            TimeseriesQuery::GroupedBasic(btsq, df, ..) => {
                let (mut basic_df, dtypes) = self.execute_basic(btsq)?;
                basic_df = basic_df
                    .join(
                        df,
                        [btsq.identifier_variable.as_ref().unwrap().as_str()],
                        [btsq.identifier_variable.as_ref().unwrap().as_str()],
                        JoinArgs::new(JoinType::Inner),
                    )
                    .unwrap();
                basic_df = basic_df
                    .drop(btsq.identifier_variable.as_ref().unwrap().as_str())
                    .unwrap();
                Ok((basic_df, dtypes))
            }
            TimeseriesQuery::ExpressionAs(tsq, v, e) => {
                let (df, dtypes) = self.execute_query_impl(tsq).await?;

                let tmp_context = Context::from_path(vec![PathEntry::Coalesce(13)]);
                let solution_mappings = SolutionMappings::new(df.lazy(), dtypes);
                let mut combiner = Combiner::new(
                    Box::new(SparqlEndpoint {
                        endpoint: "".to_string(),
                    }),
                    all_pushdowns(),
                    Box::new(TimeseriesInMemoryDatabase {
                        frames: Default::default(),
                    }),
                    vec![],
                    Default::default(),
                );
                let sm = combiner
                    .lazy_expression(e, solution_mappings, None, None, &tmp_context)
                    .await?;
                let SolutionMappings {
                    mappings,
                    rdf_node_types,
                } = extend(sm, &tmp_context, v)?;
                Ok((mappings.collect().unwrap(), rdf_node_types))
            }
        }
    }

    fn execute_basic(
        &self,
        btsq: &BasicTimeseriesQuery,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        let mut lfs = vec![];
        for id in btsq.ids.as_ref().unwrap() {
            if let Some(df) = self.frames.get(id) {
                assert!(btsq.identifier_variable.is_some());
                let mut df = df.clone();

                if let Some(value_variable) = &btsq.value_variable {
                    df.rename("value", value_variable.variable.as_str())
                        .expect("Rename problem");
                } else {
                    df = df.drop("value").expect("Drop value problem");
                }
                if let Some(timestamp_variable) = &btsq.timestamp_variable {
                    df.rename("timestamp", timestamp_variable.variable.as_str())
                        .expect("Rename problem");
                } else {
                    df = df.drop("timestamp").expect("Drop timestamp problem");
                }
                let mut lf = df.lazy();
                lf = lf.with_column(
                    lit(id.to_string()).alias(btsq.identifier_variable.as_ref().unwrap().as_str()),
                );

                lfs.push(lf);
            } else {
                panic!("Missing frame");
            }
        }
        let out_df = concat(lfs, UnionArgs::default())?.collect().unwrap();
        let dtypes = get_datatype_map(&out_df);
        Ok((out_df, dtypes))
    }

    #[async_recursion]
    async fn execute_filtered(
        &self,
        tsq: &TimeseriesQuery,
        filter: &Expression,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        let (df, dtypes) = self.execute_query_impl(tsq).await?;
        let tmp_context = Context::from_path(vec![PathEntry::Coalesce(12)]);
        let mut solution_mappings = SolutionMappings::new(df.lazy(), dtypes);
        let mut combiner = Combiner::new(
            Box::new(SparqlEndpoint {
                endpoint: "".to_string(),
            }),
            all_pushdowns(),
            Box::new(TimeseriesInMemoryDatabase {
                frames: Default::default(),
            }),
            vec![],
            Default::default(),
        );
        solution_mappings = combiner
            .lazy_expression(filter, solution_mappings, None, None, &tmp_context)
            .await?;
        let SolutionMappings{ mappings, rdf_node_types } =
            query_processing::graph_patterns::filter(solution_mappings, &tmp_context)?;
        Ok((mappings.collect().unwrap(), rdf_node_types))
    }

    async fn execute_grouped(
        &self,
        grouped: &GroupedTimeseriesQuery,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        let (df, dtypes) = self.execute_query_impl(&grouped.tsq).await?;
        let mut out_lf = df.lazy();

        let mut aggregation_exprs = vec![];
        let mut combiner = Combiner::new(
            Box::new(SparqlEndpoint {
                endpoint: "".to_string(),
            }),
            all_pushdowns(),
            Box::new(TimeseriesInMemoryDatabase {
                frames: Default::default(),
            }),
            vec![],
            Default::default(),
        );
        let mut solution_mappings = SolutionMappings::new(out_lf, dtypes);
        for i in 0..grouped.aggregations.len() {
            let (v, agg) = grouped.aggregations.get(i).unwrap();
            let agg_ctx = grouped
                .context
                .extension_with(PathEntry::GroupAggregation(i as u16));
            let AggregateReturn {
                solution_mappings: new_solution_mappings,
                expr: agg_expr,
                context: _,
                rdf_node_type,
            } = combiner
                .sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    agg,
                    solution_mappings,
                    &agg_ctx,
                )
                .await?;
            solution_mappings = new_solution_mappings;
            aggregation_exprs.push(agg_expr);
            solution_mappings
                .rdf_node_types
                .insert(v.as_str().to_string(), rdf_node_type);
        }
        let mut groupby = vec![col(grouped.tsq.get_groupby_column().unwrap())];
        let tsfuncs = grouped.tsq.get_timeseries_functions(&grouped.context);
        for b in &grouped.by {
            for (v, _) in &tsfuncs {
                if b == *v {
                    groupby.push(col(v.as_str()));
                    break;
                }
            }
        }
        let SolutionMappings {
            mappings,
            rdf_node_types,
        } = solution_mappings;

        let grouped_lf = mappings.group_by(groupby);
        out_lf = grouped_lf.agg(aggregation_exprs.as_slice());

        let collected = out_lf.collect()?;
        Ok((collected, rdf_node_types))
    }

    async fn execute_inner_synchronized(
        &self,
        inners: &Vec<Box<TimeseriesQuery>>,
        synchronizers: &Vec<Synchronizer>,
    ) -> Result<(DataFrame, HashMap<String, RDFNodeType>), Box<dyn Error>> {
        assert_eq!(synchronizers.len(), 1);
        #[allow(irrefutable_let_patterns)]
        if let Synchronizer::Identity(timestamp_col) = synchronizers.get(0).unwrap() {
            let mut on = vec![timestamp_col.clone()];
            let mut dfs = vec![];
            for q in inners {
                let (df, dtypes) = self.execute_query_impl(q).await?;
                for c in df.get_column_names() {
                    if c.starts_with(GROUPING_COL) {
                        let c_string = c.to_string();
                        if !on.contains(&c_string) {
                            on.push(c_string);
                        }
                    }
                }
                dfs.push((df, dtypes));
            }
            let (mut first_df, mut first_dtypes) = dfs.remove(0);
            for (df, dtypes) in dfs.into_iter() {
                first_df = first_df.join(
                    &df,
                    on.as_slice(),
                    on.as_slice(),
                    JoinArgs::new(JoinType::Inner),
                )?;
                first_dtypes.extend(dtypes);
            }
            Ok((first_df, first_dtypes))
        } else {
            todo!()
        }
    }
}
