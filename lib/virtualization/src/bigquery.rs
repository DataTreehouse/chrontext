use crate::errors::ChrontextError;
use crate::get_datatype_map;
use crate::python::translate_sql;
use bigquery_polars::{BigQueryExecutor, Client};
use oxrdf::Variable;
use pyo3::types::PyDict;
use pyo3::Py;
use representation::solution_mapping::EagerSolutionMappings;
use reqwest::Url;
use spargebra::algebra::{AggregateExpression, Expression, OrderExpression};
use spargebra::term::TermPattern;
use std::collections::{HashMap, HashSet};
use virtualized_query::pushdown_setting::{all_pushdowns, PushdownSetting};
use virtualized_query::{GroupedVirtualizedQuery, VirtualizedQuery};

pub struct VirtualizedBigQueryDatabase {
    gcp_sa_key: String,
    resource_sql_map: Py<PyDict>,
}

impl VirtualizedBigQueryDatabase {
    pub fn new(gcp_sa_key: String, resource_sql_map: Py<PyDict>) -> VirtualizedBigQueryDatabase {
        VirtualizedBigQueryDatabase {
            gcp_sa_key,
            resource_sql_map,
        }
    }
}

impl VirtualizedBigQueryDatabase {
    pub fn pushdown_settings() -> HashSet<PushdownSetting> {
        all_pushdowns()
    }

    pub async fn query(
        &self,
        vq: &VirtualizedQuery,
    ) -> Result<EagerSolutionMappings, ChrontextError> {
        let mut rename_map = HashMap::new();
        let new_vq = rename_non_alpha_vars(vq.clone(), &mut rename_map);
        let query_string = translate_sql(&new_vq, &self.resource_sql_map, "bigquery")?;
        // The following code is based on https://github.com/DataTreehouse/connector-x/blob/main/connectorx/src/sources/bigquery/mod.rs
        // Last modified in commit: 8134d42
        // It has been simplified and made async
        // Connector-x has the following license:
        // MIT License
        //
        // Copyright (c) 2021 SFU Database Group
        //
        // Permission is hereby granted, free of charge, to any person obtaining a copy
        // of this software and associated documentation files (the "Software"), to deal
        // in the Software without restriction, including without limitation the rights
        // to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        // copies of the Software, and to permit persons to whom the Software is
        // furnished to do so, subject to the following conditions:
        //
        // The above copyright notice and this permission notice shall be included in all
        // copies or substantial portions of the Software.
        //
        // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        // IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        // FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        // AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        // LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        // OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        // SOFTWARE.

        let url = Url::parse(&self.gcp_sa_key)?;
        let sa_key_path = url.path();
        let client = Client::from_service_account_key_file(sa_key_path).await?;

        let auth_data = std::fs::read_to_string(sa_key_path)?;
        let auth_json: serde_json::Value = serde_json::from_str(&auth_data)?;
        let project_id = auth_json
            .get("project_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        //End copied code.

        let ex = BigQueryExecutor::new(client, project_id, query_string);
        let lf = ex.execute_query().await?;
        let mut df = lf.collect().unwrap();
        for (k, v) in rename_map {
            if df.get_column_names().contains(&v.as_str()) {
                df.rename(v.as_str(), k.as_str()).unwrap();
            }
        }
        let datatypes = get_datatype_map(&df);
        Ok(EagerSolutionMappings::new(df, datatypes))
    }
}

fn rename_non_alpha_vars(
    vq: VirtualizedQuery,
    rename_map: &mut HashMap<Variable, Variable>,
) -> VirtualizedQuery {
    match vq {
        VirtualizedQuery::Basic(mut b) => {
            if let Some(v) = &b.chrontext_timestamp_variable {
                if let Some(new_v) = rename_non_alpha_var(v, rename_map) {
                    b.chrontext_timestamp_variable = Some(new_v);
                }
            }
            if let Some(v) = &b.chrontext_value_variable {
                if let Some(new_v) = rename_non_alpha_var(v, rename_map) {
                    b.chrontext_value_variable = Some(new_v);
                }
            }
            if let Some(new_v) = rename_non_alpha_var(&b.identifier_variable, rename_map) {
                b.identifier_variable = new_v;
            }
            let mut new_mappings = vec![];
            for (k, t) in &b.column_mapping {
                if let TermPattern::Variable(v) = t {
                    if let Some(new_v) = rename_non_alpha_var(v, rename_map) {
                        new_mappings.push((k.clone(), TermPattern::Variable(new_v)));
                    }
                }
            }
            for (k, t) in new_mappings {
                b.column_mapping.insert(k, t);
            }

            VirtualizedQuery::Basic(b)
        }
        VirtualizedQuery::Filtered(inner, mut expr) => {
            rename_non_alpha_expr_vars(&mut expr, rename_map);
            let new_inner = rename_non_alpha_vars(*inner, rename_map);
            VirtualizedQuery::Filtered(Box::new(new_inner), expr)
        }
        VirtualizedQuery::Ordered(inner, mut order_expressions) => {
            for o in &mut order_expressions {
                let e = match o {
                    OrderExpression::Asc(e) => e,
                    OrderExpression::Desc(e) => e,
                };
                rename_non_alpha_expr_vars(e, rename_map);
            }
            let new_inner = rename_non_alpha_vars(*inner, rename_map);
            VirtualizedQuery::Ordered(Box::new(new_inner), order_expressions)
        }
        VirtualizedQuery::InnerJoin(inners, syncs) => {
            let mut new_inners = vec![];
            for i in inners {
                new_inners.push(rename_non_alpha_vars(i, rename_map));
            }
            VirtualizedQuery::InnerJoin(new_inners, syncs)
        }
        VirtualizedQuery::ExpressionAs(vq, v, mut e) => {
            let new_vq = rename_non_alpha_vars(*vq, rename_map);
            let v = if let Some(v) = rename_non_alpha_var(&v, rename_map) {
                v
            } else {
                v
            };
            rename_non_alpha_expr_vars(&mut e, rename_map);
            VirtualizedQuery::ExpressionAs(Box::new(new_vq), v, e)
        }
        VirtualizedQuery::Grouped(GroupedVirtualizedQuery {
            context,
            vq,
            by,
            aggregations,
        }) => {
            let new_vq = rename_non_alpha_vars(*vq, rename_map);
            let mut new_by = vec![];
            for v in by {
                let v = if let Some(v) = rename_non_alpha_var(&v, rename_map) {
                    v
                } else {
                    v
                };
                new_by.push(v);
            }
            let mut new_aggregations = vec![];
            for (v, mut agg_expr) in aggregations {
                let v = if let Some(v) = rename_non_alpha_var(&v, rename_map) {
                    v
                } else {
                    v
                };
                rename_non_alpha_agg_expr_vars(&mut agg_expr, rename_map);
                new_aggregations.push((v, agg_expr));
            }
            VirtualizedQuery::Grouped(GroupedVirtualizedQuery {
                context,
                vq: Box::new(new_vq),
                by: new_by,
                aggregations: new_aggregations,
            })
        }
        VirtualizedQuery::Sliced(vq, offset, limit) => {
            let new_vq = rename_non_alpha_vars(*vq, rename_map);
            VirtualizedQuery::Sliced(Box::new(new_vq), offset, limit)
        }
    }
}

fn rename_non_alpha_expr_vars(expr: &mut Expression, rename_map: &mut HashMap<Variable, Variable>) {
    match expr {
        Expression::NamedNode(_) => {}
        Expression::Literal(_) => {}
        Expression::Variable(v) | Expression::Bound(v) => {
            if let Some(new_v) = rename_non_alpha_var(v, rename_map) {
                *v = new_v;
            }
        }
        Expression::Or(left, right)
        | Expression::And(left, right)
        | Expression::Equal(left, right)
        | Expression::SameTerm(left, right)
        | Expression::Greater(left, right)
        | Expression::GreaterOrEqual(left, right)
        | Expression::Less(left, right)
        | Expression::LessOrEqual(left, right)
        | Expression::Add(left, right)
        | Expression::Subtract(left, right)
        | Expression::Multiply(left, right)
        | Expression::Divide(left, right) => {
            rename_non_alpha_expr_vars(left, rename_map);
            rename_non_alpha_expr_vars(right, rename_map);
        }

        Expression::In(left, right) => {
            rename_non_alpha_expr_vars(left, rename_map);
            for r in right {
                rename_non_alpha_expr_vars(r, rename_map);
            }
        }
        Expression::UnaryPlus(inner) | Expression::UnaryMinus(inner) | Expression::Not(inner) => {
            rename_non_alpha_expr_vars(inner, rename_map);
        }
        Expression::Exists(_) => panic!("Should never happen"),
        Expression::If(left, middle, right) => {
            rename_non_alpha_expr_vars(left, rename_map);
            rename_non_alpha_expr_vars(middle, rename_map);
            rename_non_alpha_expr_vars(right, rename_map);
        }
        Expression::Coalesce(exprs) | Expression::FunctionCall(_, exprs) => {
            for expr in exprs {
                rename_non_alpha_expr_vars(expr, rename_map);
            }
        }
    }
}

fn rename_non_alpha_agg_expr_vars(
    agg_expr: &mut AggregateExpression,
    rename_map: &mut HashMap<Variable, Variable>,
) {
    match agg_expr {
        AggregateExpression::CountSolutions { .. } => {}
        AggregateExpression::FunctionCall { expr, .. } => {
            rename_non_alpha_expr_vars(expr, rename_map);
        }
    }
}

fn rename_non_alpha_var(
    variable: &Variable,
    rename_map: &mut HashMap<Variable, Variable>,
) -> Option<Variable> {
    if let Some(new_var) = rename_map.get(variable) {
        return Some(new_var.clone());
    }

    if !variable.as_str().chars().next().unwrap().is_alphabetic() {
        let new_id = format!("renamed_var_{}", rename_map.len());
        let new_var = Variable::new(new_id).unwrap();
        rename_map.insert(variable.clone(), new_var.clone());
        return Some(new_var);
    };
    None
}
