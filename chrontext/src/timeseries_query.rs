use crate::find_query_variables::{
    find_all_used_variables_in_expression,
};
use representation::query_context::{Context, VariableInContext};
use polars::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::Variable;
use std::collections::{HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum TimeseriesQuery {
    Basic(BasicTimeseriesQuery),
    GroupedBasic(BasicTimeseriesQuery, DataFrame, String),
    Filtered(Box<TimeseriesQuery>, Expression),
    InnerSynchronized(Vec<Box<TimeseriesQuery>>, Vec<Synchronizer>),
    ExpressionAs(Box<TimeseriesQuery>, Variable, Expression),
    Grouped(GroupedTimeseriesQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedTimeseriesQuery {
    pub context: Context, //TODO: Fix this workaround properly
    pub tsq: Box<TimeseriesQuery>,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicTimeseriesQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub resource_variable: Option<Variable>,
    pub resource: Option<String>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
}

impl BasicTimeseriesQuery {
    fn expected_columns(&self) -> HashSet<&str> {
        let mut expected_columns = HashSet::new();
        expected_columns.insert(self.identifier_variable.as_ref().unwrap().as_str());
        if let Some(vv) = &self.value_variable {
            expected_columns.insert(vv.variable.as_str());
        }
        if let Some(tsv) = &self.timestamp_variable {
            expected_columns.insert(tsv.variable.as_str());
        }
        expected_columns
    }
}

#[derive(Debug)]
pub struct TimeseriesValidationError {
    missing_columns: Vec<String>,
    extra_columns: Vec<String>,
}

impl Display for TimeseriesValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing columns: {}, Extra columns: {}",
            &self.missing_columns.join(","),
            &self.extra_columns.join(",")
        )
    }
}

impl Error for TimeseriesValidationError {}

impl TimeseriesQuery {
    pub(crate) fn has_identifiers(&self) -> bool {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(i) = &b.ids {
                    !i.is_empty()
                } else {
                    false
                }
            }
            TimeseriesQuery::GroupedBasic(_, df, _) => {
                df.height() > 0
            }
            TimeseriesQuery::Filtered(i, _) => {
                i.has_identifiers()
            }
            TimeseriesQuery::InnerSynchronized(i, _) => {
                i.iter().any(|x|x.has_identifiers())
            }
            TimeseriesQuery::ExpressionAs(t, _, _) => {t.has_identifiers()}
            TimeseriesQuery::Grouped(g) => {
                g.tsq.has_identifiers()
            }
        }
    }

    pub(crate) fn validate(&self, df: &DataFrame) -> Result<(), TimeseriesValidationError> {
        let expected_columns = self.expected_columns();
        let df_columns: HashSet<&str> = df.get_column_names().into_iter().collect();
        if expected_columns != df_columns {
            let err = TimeseriesValidationError {
                missing_columns: expected_columns
                    .difference(&df_columns)
                    .map(|x| x.to_string())
                    .collect(),
                extra_columns: df_columns
                    .difference(&expected_columns)
                    .map(|x| x.to_string())
                    .collect(),
            };
            Err(err)
        } else {
            Ok(())
        }
    }

    pub(crate) fn expected_columns<'a>(&'a self) -> HashSet<&'a str> {
        match self {
            TimeseriesQuery::Basic(b) => b.expected_columns(),
            TimeseriesQuery::Filtered(inner, ..) => inner.expected_columns(),
            TimeseriesQuery::InnerSynchronized(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, tsq| {
                    exp.extend(tsq.expected_columns());
                    exp
                })
            }
            TimeseriesQuery::Grouped(g) => {
                let mut expected_columns = HashSet::new();
                for (v, _) in &g.aggregations {
                    expected_columns.insert(v.as_str());
                }
                let tsfuncs = g.tsq.get_timeseries_functions(&g.context);
                for b in &g.by {
                    for (v, _) in &tsfuncs {
                        if b == *v {
                            expected_columns.insert(v.as_str());
                            break;
                        }
                    }
                }
                let grouping_col = self.get_groupby_column();
                expected_columns.insert(grouping_col.unwrap().as_str());
                expected_columns
            }
            TimeseriesQuery::GroupedBasic(b, _, c) => {
                let mut expected = b.expected_columns();
                expected.insert(c.as_str());
                expected.remove(b.identifier_variable.as_ref().unwrap().as_str());
                expected
            }
            TimeseriesQuery::ExpressionAs(t, ..) => t.expected_columns(),
        }
    }

    pub(crate) fn has_equivalent_value_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for value_variable in self.get_value_variables() {
            if value_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_ids(&self) -> Vec<&String> {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::Filtered(inner, _) => inner.get_ids(),
            TimeseriesQuery::InnerSynchronized(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_ids(),
            TimeseriesQuery::GroupedBasic(b, ..) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::ExpressionAs(tsq, ..) => tsq.get_ids(),
        }
    }

    pub(crate) fn get_value_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::Filtered(inner, _) => inner.get_value_variables(),
            TimeseriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_value_variables())
                }
                vs
            }
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_value_variables(),
            TimeseriesQuery::GroupedBasic(b, ..) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::ExpressionAs(t, ..) => t.get_value_variables(),
        }
    }

    pub(crate) fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::Filtered(inner, _) => inner.get_identifier_variables(),
            TimeseriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_identifier_variables(),
            TimeseriesQuery::GroupedBasic(b, ..) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::ExpressionAs(t, ..) => t.get_identifier_variables(),
        }
    }

    pub(crate) fn get_resource_variables(&self) -> Vec<&Variable> {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(res_var) = &b.resource_variable {
                    vec![res_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::Filtered(inner, _) => inner.get_resource_variables(),
            TimeseriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_resource_variables())
                }
                vs
            }
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_resource_variables(),
            TimeseriesQuery::GroupedBasic(b, ..) => {
                if let Some(res_var) = &b.resource_variable {
                    vec![res_var]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::ExpressionAs(t, ..) => t.get_resource_variables(),
        }
    }

    pub(crate) fn has_equivalent_timestamp_variable(
        &self,
        variable: &Variable,
        context: &Context,
    ) -> bool {
        for ts in self.get_timestamp_variables() {
            if ts.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_timestamp_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeseriesQuery::Basic(b) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::Filtered(t, _) => t.get_timestamp_variables(),
            TimeseriesQuery::InnerSynchronized(ts, _) => {
                let mut vs = vec![];
                for t in ts {
                    vs.extend(t.get_timestamp_variables())
                }
                vs
            }
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_timestamp_variables(),
            TimeseriesQuery::GroupedBasic(b, ..) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeseriesQuery::ExpressionAs(t, ..) => t.get_timestamp_variables(),
        }
    }
}

impl BasicTimeseriesQuery {
    pub fn new_empty() -> BasicTimeseriesQuery {
        BasicTimeseriesQuery {
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            resource_variable: None,
            resource: None,
            timestamp_variable: None,
            ids: None,
        }
    }
}

impl TimeseriesQuery {
    pub fn get_groupby_column(&self) -> Option<&String> {
        match self {
            TimeseriesQuery::Basic(..) => None,
            TimeseriesQuery::GroupedBasic(_, _, colname) => Some(colname),
            TimeseriesQuery::Filtered(tsq, _) => tsq.get_groupby_column(),
            TimeseriesQuery::InnerSynchronized(tsqs, _) => {
                let mut colname = None;
                for tsq in tsqs {
                    let new_colname = tsq.get_groupby_column();
                    if new_colname.is_some() {
                        if colname.is_some() && colname != new_colname {
                            panic!("Should never happen")
                        }
                        colname = new_colname;
                    }
                }
                colname
            }
            TimeseriesQuery::ExpressionAs(tsq, ..) => tsq.get_groupby_column(),
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_groupby_column(),
        }
    }

    pub fn get_groupby_mapping_df(&self) -> Option<&DataFrame> {
        match self {
            TimeseriesQuery::Basic(..) => None,
            TimeseriesQuery::GroupedBasic(_, df, _) => Some(df),
            TimeseriesQuery::Filtered(tsq, _) => tsq.get_groupby_mapping_df(),
            TimeseriesQuery::InnerSynchronized(tsqs, _) => {
                let mut colname = None;
                for tsq in tsqs {
                    let new_colname = tsq.get_groupby_mapping_df();
                    if new_colname.is_some() {
                        if colname.is_some() {
                            panic!("Should never happen")
                        }
                        colname = new_colname;
                    }
                }
                colname
            }
            TimeseriesQuery::ExpressionAs(tsq, ..) => tsq.get_groupby_mapping_df(),
            TimeseriesQuery::Grouped(grouped) => grouped.tsq.get_groupby_mapping_df(),
        }
    }

    pub fn get_timeseries_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            TimeseriesQuery::Basic(..) => {
                vec![]
            }
            TimeseriesQuery::GroupedBasic(..) => {
                vec![]
            }
            TimeseriesQuery::Filtered(tsq, _) => tsq.get_timeseries_functions(context),
            TimeseriesQuery::InnerSynchronized(tsqs, _) => {
                let mut out_tsfs = vec![];
                for tsq in tsqs {
                    out_tsfs.extend(tsq.get_timeseries_functions(context))
                }
                out_tsfs
            }
            TimeseriesQuery::ExpressionAs(tsq, v, e) => {
                let mut tsfs = vec![];
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars);
                let mut exists_timeseries_var = false;
                let mut all_are_timeseries_var = true;
                for v in &used_vars {
                    if tsq.has_equivalent_timestamp_variable(v, context) {
                        exists_timeseries_var = true;
                    } else {
                        all_are_timeseries_var = false;
                        break;
                    }
                }
                if exists_timeseries_var && all_are_timeseries_var {
                    tsfs.push((v, e))
                }
                tsfs.extend(tsq.get_timeseries_functions(context));
                tsfs
            }
            TimeseriesQuery::Grouped(tsq, ..) => tsq.tsq.get_timeseries_functions(context),
        }
    }
}
