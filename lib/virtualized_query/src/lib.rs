pub mod pushdown_setting;
pub mod python;

use polars::frame::DataFrame;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::query_context::{Context, VariableInContext};
use serde::{Deserialize, Serialize};
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::Variable;
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use polars::export::ahash::HashMap;
use templates::ast::Template;

#[derive(Clone, Debug)]
pub struct Virtualization {
    // Used to identify the table of the time series value
    pub resource_name: String,
    pub template: Template,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VirtualizedQuery {
    Basic(BasicVirtualizedQuery),
    GroupedBasic(BasicVirtualizedQuery, DataFrame, String),
    Filtered(Box<VirtualizedQuery>, Expression),
    InnerSynchronized(Vec<Box<VirtualizedQuery>>, Vec<Synchronizer>),
    ExpressionAs(Box<VirtualizedQuery>, Variable, Expression),
    Grouped(GroupedVirtualizedQuery),
    Limited(Box<VirtualizedQuery>, usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedVirtualizedQuery {
    pub context: Context, //TODO: Fix this workaround properly
    pub vq: Box<VirtualizedQuery>,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicVirtualizedQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub resource_variable: Option<Variable>,
    pub resource: Option<String>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
}

impl BasicVirtualizedQuery {
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

//TODO: Redo these recursions in one method..
impl VirtualizedQuery {
    pub fn has_identifiers(&self) -> bool {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(i) = &b.ids {
                    !i.is_empty()
                } else {
                    false
                }
            }
            VirtualizedQuery::GroupedBasic(_, df, _) => df.height() > 0,
            VirtualizedQuery::Filtered(i, _) => i.has_identifiers(),
            VirtualizedQuery::InnerSynchronized(i, _) => i.iter().any(|x| x.has_identifiers()),
            VirtualizedQuery::ExpressionAs(t, _, _) => t.has_identifiers(),
            VirtualizedQuery::Grouped(g) => g.vq.has_identifiers(),
            VirtualizedQuery::Limited(i, _) => i.has_identifiers(),
        }
    }

    pub fn validate(&self, df: &DataFrame) -> Result<(), TimeseriesValidationError> {
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

    pub fn expected_columns<'a>(&'a self) -> HashSet<&'a str> {
        match self {
            VirtualizedQuery::Basic(b) => b.expected_columns(),
            VirtualizedQuery::Filtered(inner, ..) => inner.expected_columns(),
            VirtualizedQuery::InnerSynchronized(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, vq| {
                    exp.extend(vq.expected_columns());
                    exp
                })
            }
            VirtualizedQuery::Grouped(g) => {
                let mut expected_columns = HashSet::new();
                for (v, _) in &g.aggregations {
                    expected_columns.insert(v.as_str());
                }
                let tsfuncs = g.vq.get_timeseries_functions(&g.context);
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
            VirtualizedQuery::GroupedBasic(b, _, c) => {
                let mut expected = b.expected_columns();
                expected.insert(c.as_str());
                expected.remove(b.identifier_variable.as_ref().unwrap().as_str());
                expected
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.expected_columns(),
            VirtualizedQuery::Limited(inner, ..) => inner.expected_columns(),
        }
    }

    pub fn has_equivalent_value_variable(&self, variable: &Variable, context: &Context) -> bool {
        for value_variable in self.get_value_variables() {
            if value_variable.equivalent(variable, context) {
                return true;
            }
        }
        false
    }

    pub fn get_ids(&self) -> Vec<&String> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(inner, _) => inner.get_ids(),
            VirtualizedQuery::InnerSynchronized(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_ids(),
            VirtualizedQuery::GroupedBasic(b, ..) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::ExpressionAs(vq, ..) => vq.get_ids(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_ids(),
        }
    }

    pub fn get_value_variables(&self) -> Vec<&VariableInContext> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(inner, _) => inner.get_value_variables(),
            VirtualizedQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_value_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_value_variables(),
            VirtualizedQuery::GroupedBasic(b, ..) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_value_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_value_variables(),
        }
    }

    pub fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(inner, _) => inner.get_identifier_variables(),
            VirtualizedQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_identifier_variables(),
            VirtualizedQuery::GroupedBasic(b, ..) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_identifier_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_identifier_variables(),
        }
    }

    pub fn get_resource_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(res_var) = &b.resource_variable {
                    vec![res_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(inner, _) => inner.get_resource_variables(),
            VirtualizedQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_resource_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_resource_variables(),
            VirtualizedQuery::GroupedBasic(b, ..) => {
                if let Some(res_var) = &b.resource_variable {
                    vec![res_var]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_resource_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_resource_variables(),
        }
    }

    pub fn has_equivalent_timestamp_variable(
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

    pub fn get_timestamp_variables(&self) -> Vec<&VariableInContext> {
        match self {
            VirtualizedQuery::Basic(b) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::Filtered(t, _) => t.get_timestamp_variables(),
            VirtualizedQuery::InnerSynchronized(ts, _) => {
                let mut vs = vec![];
                for t in ts {
                    vs.extend(t.get_timestamp_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_timestamp_variables(),
            VirtualizedQuery::GroupedBasic(b, ..) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_timestamp_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_timestamp_variables(),
        }
    }
}

impl BasicVirtualizedQuery {
    pub fn new_empty() -> BasicVirtualizedQuery {
        BasicVirtualizedQuery {
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

impl VirtualizedQuery {
    pub fn get_groupby_column(&self) -> Option<&String> {
        match self {
            VirtualizedQuery::Basic(..) => None,
            VirtualizedQuery::GroupedBasic(_, _, colname) => Some(colname),
            VirtualizedQuery::Filtered(vq, _) => vq.get_groupby_column(),
            VirtualizedQuery::InnerSynchronized(vqs, _) => {
                let mut colname = None;
                for vq in vqs {
                    let new_colname = vq.get_groupby_column();
                    if new_colname.is_some() {
                        if colname.is_some() && colname != new_colname {
                            panic!("Should never happen")
                        }
                        colname = new_colname;
                    }
                }
                colname
            }
            VirtualizedQuery::ExpressionAs(vq, ..) => vq.get_groupby_column(),
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_groupby_column(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_groupby_column(),
        }
    }

    pub fn get_groupby_mapping_df(&self) -> Option<&DataFrame> {
        match self {
            VirtualizedQuery::Basic(..) => None,
            VirtualizedQuery::GroupedBasic(_, df, _) => Some(df),
            VirtualizedQuery::Filtered(vq, _) => vq.get_groupby_mapping_df(),
            VirtualizedQuery::InnerSynchronized(vqs, _) => {
                let mut colname = None;
                for vq in vqs {
                    let new_colname = vq.get_groupby_mapping_df();
                    if new_colname.is_some() {
                        if colname.is_some() {
                            panic!("Should never happen")
                        }
                        colname = new_colname;
                    }
                }
                colname
            }
            VirtualizedQuery::ExpressionAs(vq, ..) => vq.get_groupby_mapping_df(),
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_groupby_mapping_df(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_groupby_mapping_df(),
        }
    }

    pub fn get_timeseries_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            VirtualizedQuery::Basic(..) => {
                vec![]
            }
            VirtualizedQuery::GroupedBasic(..) => {
                vec![]
            }
            VirtualizedQuery::Filtered(vq, _) => vq.get_timeseries_functions(context),
            VirtualizedQuery::InnerSynchronized(vqs, _) => {
                let mut out_tsfs = vec![];
                for vq in vqs {
                    out_tsfs.extend(vq.get_timeseries_functions(context))
                }
                out_tsfs
            }
            VirtualizedQuery::ExpressionAs(vq, v, e) => {
                let mut tsfs = vec![];
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars);
                let mut exists_timeseries_var = false;
                let mut all_are_timeseries_var = true;
                for v in &used_vars {
                    if vq.has_equivalent_timestamp_variable(v, context) {
                        exists_timeseries_var = true;
                    } else {
                        all_are_timeseries_var = false;
                        break;
                    }
                }
                if exists_timeseries_var && all_are_timeseries_var {
                    tsfs.push((v, e))
                }
                tsfs.extend(vq.get_timeseries_functions(context));
                tsfs
            }
            VirtualizedQuery::Grouped(vq, ..) => vq.vq.get_timeseries_functions(context),
            VirtualizedQuery::Limited(inner, ..) => inner.get_timeseries_functions(context),
        }
    }
}
