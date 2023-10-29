use crate::combiner::solution_mapping::SolutionMappings;
use crate::combiner::Combiner;
use crate::constants::GROUPING_COL;
use crate::pushdown_setting::all_pushdowns;
use crate::query_context::{Context, PathEntry};
use crate::sparql_database::sparql_endpoint::SparqlEndpoint;
use crate::timeseries_database::TimeSeriesQueryable;
use crate::timeseries_query::{
    BasicTimeSeriesQuery, GroupedTimeSeriesQuery, Synchronizer, TimeSeriesQuery,
};
use async_recursion::async_recursion;
use async_trait::async_trait;
use polars::frame::DataFrame;
use polars::prelude::DataFrameJoinOps;
use polars::prelude::{col, concat, lit, IntoLazy, UnionArgs};
use polars_core::prelude::{JoinArgs, JoinType};
use spargebra::algebra::Expression;
use std::collections::HashMap;
use std::error::Error;

pub struct InMemoryTimeseriesDatabase {
    pub frames: HashMap<String, DataFrame>,
}

#[async_trait]
impl TimeSeriesQueryable for InMemoryTimeseriesDatabase {
    async fn execute(&mut self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        self.execute_query(tsq).await
    }

    fn allow_compound_timeseries_queries(&self) -> bool {
        true
    }
}

impl InMemoryTimeseriesDatabase {
    #[async_recursion]
    async fn execute_query(&self, tsq: &TimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
        match tsq {
            TimeSeriesQuery::Basic(b) => self.execute_basic(b),
            TimeSeriesQuery::Filtered(inner, filter) => self.execute_filtered(inner, filter).await,
            TimeSeriesQuery::InnerSynchronized(inners, synchronizers) => {
                self.execute_inner_synchronized(inners, synchronizers).await
            }
            TimeSeriesQuery::Grouped(grouped) => self.execute_grouped(grouped).await,
            TimeSeriesQuery::GroupedBasic(btsq, df, ..) => {
                let mut basic_df = self.execute_basic(btsq)?;
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
                Ok(basic_df)
            }
            TimeSeriesQuery::ExpressionAs(tsq, v, e) => {
                let mut df = self.execute_query(tsq).await?;
                let tmp_context = Context::from_path(vec![PathEntry::Coalesce(13)]);
                let columns = df
                    .get_column_names()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect();
                let solution_mappings = SolutionMappings::new(df.lazy(), columns, HashMap::new());
                let mut combiner = Combiner::new(
                    Box::new(SparqlEndpoint {
                        endpoint: "".to_string(),
                    }),
                    all_pushdowns(),
                    Box::new(InMemoryTimeseriesDatabase {
                        frames: Default::default(),
                    }),
                    vec![],
                    Default::default(),
                );
                let mut out_lf = combiner
                    .lazy_expression(e, solution_mappings, None, None, &tmp_context)
                    .await?;
                out_lf.mappings = out_lf.mappings.rename([tmp_context.as_str()], [v.as_str()]);
                df = out_lf.mappings.collect().unwrap();
                Ok(df)
            }
        }
    }

    fn execute_basic(&self, btsq: &BasicTimeSeriesQuery) -> Result<DataFrame, Box<dyn Error>> {
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
        let out_lf = concat(lfs, UnionArgs::default())?;
        Ok(out_lf.collect().unwrap())
    }

    #[async_recursion]
    async fn execute_filtered(
        &self,
        tsq: &TimeSeriesQuery,
        filter: &Expression,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let df = self.execute_query(tsq).await?;
        let columns = df
            .get_column_names()
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        let tmp_context = Context::from_path(vec![PathEntry::Coalesce(12)]);
        let mut solution_mappings = SolutionMappings::new(df.lazy(), columns, HashMap::new());
        let mut combiner = Combiner::new(
            Box::new(SparqlEndpoint {
                endpoint: "".to_string(),
            }),
            all_pushdowns(),
            Box::new(InMemoryTimeseriesDatabase {
                frames: Default::default(),
            }),
            vec![],
            Default::default(),
        );
        solution_mappings = combiner
            .lazy_expression(filter, solution_mappings, None, None, &tmp_context)
            .await?;
        solution_mappings.mappings = solution_mappings
            .mappings
            .filter(col(tmp_context.as_str()))
            .drop_columns([tmp_context.as_str()]);
        Ok(solution_mappings.mappings.collect().unwrap())
    }

    async fn execute_grouped(
        &self,
        grouped: &GroupedTimeSeriesQuery,
    ) -> Result<DataFrame, Box<dyn Error>> {
        let df = self.execute_query(&grouped.tsq).await?;
        let columns = df
            .get_column_names()
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        let mut out_lf = df.lazy();

        let mut aggregation_exprs = vec![];
        let mut combiner = Combiner::new(
            Box::new(SparqlEndpoint {
                endpoint: "".to_string(),
            }),
            all_pushdowns(),
            Box::new(InMemoryTimeseriesDatabase {
                frames: Default::default(),
            }),
            vec![],
            Default::default(),
        );
        let mut solution_mappings = SolutionMappings::new(out_lf, columns, HashMap::new());
        for i in 0..grouped.aggregations.len() {
            let (v, agg) = grouped.aggregations.get(i).unwrap();
            let (new_solution_mappings, agg_expr, _) = combiner
                .sparql_aggregate_expression_as_lazy_column_and_expression(
                    v,
                    agg,
                    solution_mappings,
                    &grouped
                        .context
                        .extension_with(PathEntry::GroupAggregation(i as u16)),
                )
                .await?;
            solution_mappings = new_solution_mappings;
            aggregation_exprs.push(agg_expr);
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

        let grouped_lf = solution_mappings.mappings.groupby(groupby);
        out_lf = grouped_lf.agg(aggregation_exprs.as_slice());

        let collected = out_lf.collect()?;
        Ok(collected)
    }

    async fn execute_inner_synchronized(
        &self,
        inners: &Vec<Box<TimeSeriesQuery>>,
        synchronizers: &Vec<Synchronizer>,
    ) -> Result<DataFrame, Box<dyn Error>> {
        assert_eq!(synchronizers.len(), 1);
        #[allow(irrefutable_let_patterns)]
        if let Synchronizer::Identity(timestamp_col) = synchronizers.get(0).unwrap() {
            let mut on = vec![timestamp_col.clone()];
            let mut dfs = vec![];
            for q in inners {
                let df = self.execute_query(q).await?;
                for c in df.get_column_names() {
                    if c.starts_with(GROUPING_COL) {
                        let c_string = c.to_string();
                        if !on.contains(&c_string) {
                            on.push(c_string);
                        }
                    }
                }
                dfs.push(df);
            }
            let mut first_df = dfs.remove(0);
            for df in dfs.into_iter() {
                first_df = first_df.join(
                    &df,
                    on.as_slice(),
                    on.as_slice(),
                    JoinArgs::new(JoinType::Inner),
                )?;
            }
            Ok(first_df)
        } else {
            todo!()
        }
    }
}
