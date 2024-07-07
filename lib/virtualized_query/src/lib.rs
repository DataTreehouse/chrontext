pub mod pushdown_setting;
pub mod python;

use polars::export::ahash::HashMap;
use polars::frame::DataFrame;
use query_processing::find_query_variables::find_all_used_variables_in_expression;
use representation::query_context::{Context, VariableInContext};
use spargebra::algebra::{AggregateExpression, Expression};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use std::collections::HashSet;
use std::error::Error;
use std::fmt::{Display, Formatter};
use templates::ast::{ConstantTerm, ConstantTermOrList, StottrTerm, Template};

pub const ID_VARIABLE_NAME: &str = "id";

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
    pub identifier_variable: Variable,
    pub column_mapping: HashMap<String, TermPattern>,
    pub resource_variable: Variable,
    pub query_source_context: Context,
    pub query_source_variable: Variable,
    pub resource: Option<String>,
    pub ids: Option<Vec<String>>,
}

impl BasicVirtualizedQuery {
    pub fn finish_column_mapping(&mut self, patterns: &Vec<TriplePattern>, template: &Template) {
        let mut new_mappings = vec![];
        let mut visited_query_vars = HashSet::new();

        let mut queue = vec![(&self.query_source_variable, ID_VARIABLE_NAME)];
        while !queue.is_empty() {
            let (current_query_var, current_template_var_name) = queue.pop().unwrap();
            if !visited_query_vars.contains(&current_query_var) {
                visited_query_vars.insert(current_query_var);
                for p in patterns {
                    match &p.predicate {
                        NamedNodePattern::NamedNode(nn) => {
                            if let TermPattern::Variable(v) = &p.subject {
                                if current_query_var == v {
                                    for tp in &template.pattern_list {
                                        if let StottrTerm::ConstantTerm(
                                            ConstantTermOrList::ConstantTerm(ConstantTerm::Iri(
                                                template_nn,
                                            )),
                                        ) = &tp.argument_list.get(1).unwrap().term
                                        {
                                            if nn == template_nn {
                                                match &tp.argument_list.get(0).unwrap().term {
                                                    StottrTerm::Variable(tv) => {
                                                        if tv.name.as_str()
                                                            == current_template_var_name
                                                        {
                                                            match &tp
                                                                .argument_list
                                                                .get(2)
                                                                .unwrap()
                                                                .term
                                                            {
                                                                StottrTerm::Variable(tobj) => {
                                                                    new_mappings.push((
                                                                        tobj.name.clone(),
                                                                        p.object.clone(),
                                                                    ));
                                                                    if let TermPattern::Variable(
                                                                        obj,
                                                                    ) = &p.object
                                                                    {
                                                                        queue.push((
                                                                            obj,
                                                                            tobj.name.as_str(),
                                                                        ));
                                                                    }
                                                                }
                                                                StottrTerm::ConstantTerm(_) => {}
                                                                StottrTerm::List(_) => {}
                                                            }
                                                        }
                                                    }
                                                    StottrTerm::ConstantTerm(_) => {}
                                                    StottrTerm::List(_) => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        NamedNodePattern::Variable(_) => {
                            //Need to check if subject is external.. and then do something..
                        }
                    }
                }
            }
        }
        self.column_mapping.extend(new_mappings);
    }
}

impl BasicVirtualizedQuery {
    fn expected_columns(&self) -> HashSet<&str> {
        let mut s = HashSet::new();
        for v in self.column_mapping.keys() {
            s.insert(v.as_str());
        }
        s
    }

    pub fn get_virtualized_variables(&self) -> Vec<VariableInContext> {
        let mut virt = vec![];
        for vc in self.column_mapping.values() {
            if let TermPattern::Variable(vt) = vc {
                virt.push(VariableInContext::new(
                    vt.clone(),
                    self.query_source_context.clone(),
                ));
            }
        }
        virt
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

    pub fn expected_columns(&self) -> HashSet<&str> {
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
                let tsfuncs = g.vq.get_virtualized_functions(&g.context);
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
                expected.remove(b.identifier_variable.as_str());
                expected
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.expected_columns(),
            VirtualizedQuery::Limited(inner, ..) => inner.expected_columns(),
        }
    }
    //
    // pub fn has_equivalent_value_variable(&self, variable: &Variable, context: &Context) -> bool {
    //     for value_variable in self.get_value_variables() {
    //         if value_variable.equivalent(variable, context) {
    //             return true;
    //         }
    //     }
    //     false
    // }

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

    pub fn get_virtualized_variables(&self) -> Vec<VariableInContext> {
        match self {
            VirtualizedQuery::Basic(b) => b.get_virtualized_variables(),
            VirtualizedQuery::Filtered(inner, _) => inner.get_virtualized_variables(),
            VirtualizedQuery::InnerSynchronized(inners, _) => {
                let mut vs = vec![];
                for inner in inners {
                    vs.extend(inner.get_virtualized_variables())
                }
                vs
            }
            VirtualizedQuery::Grouped(grouped) => grouped.vq.get_virtualized_variables(),
            VirtualizedQuery::GroupedBasic(b, ..) => b.get_virtualized_variables(),
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_virtualized_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_virtualized_variables(),
        }
    }

    pub fn get_identifier_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                vec![&b.identifier_variable]
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
                vec![&b.identifier_variable]
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_identifier_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_identifier_variables(),
        }
    }

    pub fn get_resource_variables(&self) -> Vec<&Variable> {
        match self {
            VirtualizedQuery::Basic(b) => {
                vec![&b.resource_variable]
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
                vec![&b.resource_variable]
            }
            VirtualizedQuery::ExpressionAs(t, ..) => t.get_resource_variables(),
            VirtualizedQuery::Limited(inner, ..) => inner.get_resource_variables(),
        }
    }

    pub fn has_equivalent_variable(&self, variable: &Variable, context: &Context) -> bool {
        for ts in self.get_virtualized_variables() {
            if ts.equivalent(variable, context) {
                return true;
            }
        }
        false
    }
}

impl BasicVirtualizedQuery {
    pub fn new(
        query_source_context: Context,
        query_source_variable: Variable,
        identifier_variable: Variable,
        resource_variable: Variable,
    ) -> BasicVirtualizedQuery {
        BasicVirtualizedQuery {
            identifier_variable,
            column_mapping: Default::default(),
            resource_variable,
            query_source_context,
            query_source_variable,
            resource: None,
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

    pub fn get_virtualized_functions(&self, context: &Context) -> Vec<(&Variable, &Expression)> {
        match self {
            VirtualizedQuery::Basic(..) => {
                vec![]
            }
            VirtualizedQuery::GroupedBasic(..) => {
                vec![]
            }
            VirtualizedQuery::Filtered(vq, _) => vq.get_virtualized_functions(context),
            VirtualizedQuery::InnerSynchronized(vqs, _) => {
                let mut out_tsfs = vec![];
                for vq in vqs {
                    out_tsfs.extend(vq.get_virtualized_functions(context))
                }
                out_tsfs
            }
            VirtualizedQuery::ExpressionAs(vq, v, e) => {
                let mut tsfs = vec![];
                let mut used_vars = HashSet::new();
                find_all_used_variables_in_expression(e, &mut used_vars);
                let mut exists_virtalized_var = false;
                let mut all_are_virtualized_var = true;
                for v in &used_vars {
                    if vq.has_equivalent_variable(v, context) {
                        exists_virtalized_var = true;
                    } else {
                        all_are_virtualized_var = false;
                        break;
                    }
                }
                if exists_virtalized_var && all_are_virtualized_var {
                    tsfs.push((v, e))
                }
                tsfs.extend(vq.get_virtualized_functions(context));
                tsfs
            }
            VirtualizedQuery::Grouped(vq, ..) => vq.vq.get_virtualized_functions(context),
            VirtualizedQuery::Limited(inner, ..) => inner.get_virtualized_functions(context),
        }
    }
}
