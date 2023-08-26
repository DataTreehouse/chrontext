use crate::find_query_variables::{find_all_used_variables_in_aggregate_expression, find_all_used_variables_in_expression};
use crate::query_context::{Context, VariableInContext};
use oxrdf::NamedNode;
use polars::frame::DataFrame;
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::Variable;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter};
use log::warn;
use oxrdf::vocab::xsd;

#[derive(Debug, Clone, PartialEq)]
pub enum TimeSeriesQuery {
    Basic(BasicTimeSeriesQuery),
    GroupedBasic(BasicTimeSeriesQuery, DataFrame, String),
    Filtered(Box<TimeSeriesQuery>, Expression), 
    InnerSynchronized(Vec<Box<TimeSeriesQuery>>, Vec<Synchronizer>),
    ExpressionAs(Box<TimeSeriesQuery>, Variable, Expression),
    Grouped(GroupedTimeSeriesQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Synchronizer {
    Identity(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupedTimeSeriesQuery {
    pub context: Context, //TODO: Fix this workaround properly
    pub tsq: Box<TimeSeriesQuery>,
    pub by: Vec<Variable>,
    pub aggregations: Vec<(Variable, AggregateExpression)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasicTimeSeriesQuery {
    pub identifier_variable: Option<Variable>,
    pub timeseries_variable: Option<VariableInContext>,
    pub data_point_variable: Option<VariableInContext>,
    pub value_variable: Option<VariableInContext>,
    pub datatype_variable: Option<Variable>,
    pub datatype: Option<NamedNode>,
    pub resource_variable: Option<Variable>,
    pub resource: Option<String>,
    pub timestamp_variable: Option<VariableInContext>,
    pub ids: Option<Vec<String>>,
}

impl BasicTimeSeriesQuery {
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
pub struct TimeSeriesValidationError {
    missing_columns: Vec<String>,
    extra_columns: Vec<String>,
}

impl Display for TimeSeriesValidationError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Missing columns: {}, Extra columns: {}",
            &self.missing_columns.join(","),
            &self.extra_columns.join(",")
        )
    }
}

impl Error for TimeSeriesValidationError {}

impl TimeSeriesQuery {
    pub(crate) fn validate(&self, df: &DataFrame) -> Result<(), TimeSeriesValidationError> {
        let expected_columns = self.expected_columns();
        let df_columns: HashSet<&str> = df.get_column_names().into_iter().collect();
        if expected_columns != df_columns {
            let err = TimeSeriesValidationError {
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

    fn expected_columns<'a>(&'a self) -> HashSet<&'a str> {
        match self {
            TimeSeriesQuery::Basic(b) => b.expected_columns(),
            TimeSeriesQuery::Filtered(inner, ..) => inner.expected_columns(),
            TimeSeriesQuery::InnerSynchronized(inners, _synchronizers) => {
                inners.iter().fold(HashSet::new(), |mut exp, tsq| {
                    exp.extend(tsq.expected_columns());
                    exp
                })
            }
            TimeSeriesQuery::Grouped(g) => {
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
            TimeSeriesQuery::GroupedBasic(b, _, c) => {
                let mut expected = b.expected_columns();
                expected.insert(c.as_str());
                expected.remove(b.identifier_variable.as_ref().unwrap().as_str());
                expected
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.expected_columns(),
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
            TimeSeriesQuery::Basic(b) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_ids(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut ss = vec![];
                for inner in inners {
                    ss.extend(inner.get_ids())
                }
                ss
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_ids(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(ids) = &b.ids {
                    ids.iter().collect()
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(tsq, ..) => tsq.get_ids(),
        }
    }

    pub(crate) fn get_value_variables(&self) -> Vec<&VariableInContext> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_value_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_value_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_value_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(val_var) = &b.value_variable {
                    vec![val_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_value_variables(),
        }
    }

    pub(crate) fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_identifier_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_identifier_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_identifier_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(id_var) = &b.identifier_variable {
                    vec![id_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_identifier_variables(),
        }
    }
    
    pub(crate) fn get_datatype_variables(&self) -> Vec<&Variable> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                if let Some(dt_var) = &b.datatype_variable {
                    vec![dt_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(inner, _) => inner.get_datatype_variables(),
            TimeSeriesQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_datatype_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_datatype_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(dt_var) = &b.datatype_variable {
                    vec![dt_var]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_datatype_variables(),
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
            TimeSeriesQuery::Basic(b) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::Filtered(t, _) => t.get_timestamp_variables(),
            TimeSeriesQuery::InnerSynchronized(ts, _) => {
                let mut vs = vec![];
                for t in ts {
                    vs.extend(t.get_timestamp_variables())
                }
                vs
            }
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_timestamp_variables(),
            TimeSeriesQuery::GroupedBasic(b, ..) => {
                if let Some(v) = &b.timestamp_variable {
                    vec![v]
                } else {
                    vec![]
                }
            }
            TimeSeriesQuery::ExpressionAs(t, ..) => t.get_timestamp_variables(),
        }
    }
}

impl BasicTimeSeriesQuery {
    pub fn new_empty() -> BasicTimeSeriesQuery {
        BasicTimeSeriesQuery {
            identifier_variable: None,
            timeseries_variable: None,
            data_point_variable: None,
            value_variable: None,
            datatype_variable: None,
            datatype: None,
            resource_variable: None,
            resource: None,
            timestamp_variable: None,
            ids: None,
        }
    }
}

impl TimeSeriesQuery {
    pub fn get_groupby_column(&self) -> Option<&String> {
        match self {
            TimeSeriesQuery::Basic(..) => None,
            TimeSeriesQuery::GroupedBasic(_, _, colname) => Some(colname),
            TimeSeriesQuery::Filtered(tsq, _) => tsq.get_groupby_column(),
            TimeSeriesQuery::InnerSynchronized(tsqs, _) => {
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
            TimeSeriesQuery::ExpressionAs(tsq, ..) => tsq.get_groupby_column(),
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_groupby_column(),
        }
    }

    pub fn get_groupby_mapping_df(&self) -> Option<&DataFrame> {
        match self {
            TimeSeriesQuery::Basic(..) => None,
            TimeSeriesQuery::GroupedBasic(_, df, _) => Some(df),
            TimeSeriesQuery::Filtered(tsq, _) => tsq.get_groupby_mapping_df(),
            TimeSeriesQuery::InnerSynchronized(tsqs, _) => {
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
            TimeSeriesQuery::ExpressionAs(tsq, ..) => tsq.get_groupby_mapping_df(),
            TimeSeriesQuery::Grouped(grouped) => grouped.tsq.get_groupby_mapping_df(),
        }
    }

    pub fn get_timeseries_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            TimeSeriesQuery::Basic(..) => {
                vec![]
            }
            TimeSeriesQuery::GroupedBasic(..) => {
                vec![]
            }
            TimeSeriesQuery::Filtered(tsq, _) => tsq.get_timeseries_functions(context),
            TimeSeriesQuery::InnerSynchronized(tsqs, _) => {
                let mut out_tsfs = vec![];
                for tsq in tsqs {
                    out_tsfs.extend(tsq.get_timeseries_functions(context))
                }
                out_tsfs
            }
            TimeSeriesQuery::ExpressionAs(tsq, v, e) => {
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
            TimeSeriesQuery::Grouped(tsq, ..) => tsq.tsq.get_timeseries_functions(context),
        }
    }


    pub fn get_datatype_map(&self) -> HashMap<String, NamedNode> {
        match self {
            TimeSeriesQuery::Basic(b) => {
                let mut map = HashMap::new();
                if let Some(tsv) = &b.timestamp_variable {
                    map.insert(tsv.variable.as_str().to_string(), xsd::DATE_TIME_STAMP.into_owned());
                }
                if let Some(v) = &b.value_variable.clone() {
                    map.insert(v.variable.as_str().to_string(), b.datatype.as_ref().unwrap().clone());
                }
                map
            }
            TimeSeriesQuery::GroupedBasic(b, .. ) => {
                HashMap::from([(b.value_variable.as_ref().unwrap().variable.as_str().to_string(), b.datatype.as_ref().unwrap().clone())])
            }
            TimeSeriesQuery::Filtered(tsq, _) => tsq.get_datatype_map(),
            TimeSeriesQuery::InnerSynchronized(tsqs, _) => {
                let mut map = HashMap::new();
                for tsq in tsqs {
                    map.extend(tsq.get_datatype_map());
                }
                map
            }
            TimeSeriesQuery::ExpressionAs(tsq, v, e) => {
                let v_str = v.as_str();
                let mut map = tsq.get_datatype_map();
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars);
                for u in &used_vars {
                    let u_str = u.as_str();
                    if map.contains_key(u_str) {
                        map.insert(v_str.to_string(), map.get(u_str).unwrap().clone());
                    } else {
                        warn!("Map does not contain datatype {:?}", u);
                    }
                }
                map
            }
            TimeSeriesQuery::Grouped(gr ) => {
                let mut map = gr.tsq.get_datatype_map();
                for (v,agg) in gr.aggregations.iter().rev() {
                    let v_str = v.as_str();
                    let mut used_vars = HashSet::new();
                    find_all_used_variables_in_aggregate_expression(&agg, &mut used_vars);
                    for av in used_vars {
                        let av_str = av.as_str();
                        //TODO: This is not correct.
                        map.insert(v_str.to_string(), map.get(av_str).unwrap().clone());
                    }
                }
                map
            }
        }
    }
}
