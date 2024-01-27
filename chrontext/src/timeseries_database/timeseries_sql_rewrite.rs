mod expression_rewrite;
mod partitioning_support;

use crate::timeseries_database::timeseries_sql_rewrite::expression_rewrite::SPARQLToSQLExpressionTransformer;
use crate::timeseries_database::timeseries_sql_rewrite::partitioning_support::add_partitioned_timestamp_conditions;
use crate::timeseries_database::DatabaseType;
use crate::timeseries_query::{BasicTimeseriesQuery, Synchronizer, TimeseriesQuery};
use oxrdf::{Variable};
use polars_core::datatypes::AnyValue;
use polars_core::frame::DataFrame;
use sea_query::extension::bigquery::{NamedField, Unnest};
use sea_query::IntoIden;
use sea_query::{
    Alias, BinOper, ColumnRef, JoinType, Order, Query, SelectStatement, SimpleExpr, TableRef,
};
use sea_query::{Expr as SeaExpr, Iden, Value};
use spargebra::algebra::{AggregateExpression, Expression};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Display, Formatter, Write};

const YEAR_PARTITION_COLUMN_NAME: &str = "year_partition_column_name";
const MONTH_PARTITION_COLUMN_NAME: &str = "month_partition_column_name";
const DAY_PARTITION_COLUMN_NAME: &str = "day_partition_column_name";

#[derive(Debug)]
pub enum TimeseriesQueryToSQLError {
    UnknownVariable(String),
    UnknownDatatype(String),
    FoundNonValueInInExpression,
    DatatypeNotSupported(String),
    MissingTimeseriesResource,
    TimeseriesResourceNotFound(String, Vec<String>),
}

impl Display for TimeseriesQueryToSQLError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeseriesQueryToSQLError::UnknownVariable(v) => {
                write!(f, "Unknown variable {}", v)
            }
            TimeseriesQueryToSQLError::UnknownDatatype(d) => {
                write!(f, "Unknown datatype: {}", d)
            }
            TimeseriesQueryToSQLError::FoundNonValueInInExpression => {
                write!(f, "In-expression contained non-literal alternative")
            }
            TimeseriesQueryToSQLError::DatatypeNotSupported(dt) => {
                write!(f, "Datatype not supported: {}", dt)
            }
            TimeseriesQueryToSQLError::MissingTimeseriesResource => {
                write!(f, "Timeseries value resource name missing")
            }
            TimeseriesQueryToSQLError::TimeseriesResourceNotFound(resource, alternatives) => {
                write!(
                    f,
                    "Timeseries resource {} not found among alternatives {}",
                    resource,
                    alternatives.join(",")
                )
            }
        }
    }
}

impl Error for TimeseriesQueryToSQLError {}

#[derive(Clone)]
pub(crate) enum Name {
    Schema(String),
    Table(String),
    Column(String),
    Function(String),
}

impl Iden for Name {
    fn unquoted(&self, s: &mut dyn Write) {
        write!(
            s,
            "{}",
            match self {
                Name::Schema(s) => {
                    s
                }
                Name::Table(s) => {
                    s
                }
                Name::Column(s) => {
                    s
                }
                Name::Function(s) => {
                    s
                }
            }
        )
        .unwrap();
    }
}

#[derive(Clone)]
pub struct TimeseriesTable {
    // Used to identify the table of the time series value
    pub resource_name: String,
    pub schema: Option<String>,
    pub time_series_table: String,
    pub value_column: String,
    pub timestamp_column: String,
    pub identifier_column: String,
    pub year_column: Option<String>,
    pub month_column: Option<String>,
    pub day_column: Option<String>,
}

pub struct TimeseriesQueryToSQLTransformer<'a> {
    pub partition_support: bool,
    pub tables: &'a Vec<TimeseriesTable>,
    pub database_type: DatabaseType,
}

impl TimeseriesQueryToSQLTransformer<'_> {
    pub fn new(
        tables: &Vec<TimeseriesTable>,
        database_type: DatabaseType,
    ) -> TimeseriesQueryToSQLTransformer {
        TimeseriesQueryToSQLTransformer {
            partition_support: check_partitioning_support(tables),
            tables,
            database_type,
        }
    }

    pub fn create_query(
        &self,
        tsq: &TimeseriesQuery,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        let (mut select_statement, map) = self.create_query_nested(tsq, project_date_partition)?;
        let sort_col;
        if let Some(grcol) = tsq.get_groupby_column() {
            sort_col = grcol.clone();
        } else {
            let idvars = tsq.get_identifier_variables();
            assert_eq!(idvars.len(), 1);
            sort_col = idvars.get(0).unwrap().as_str().to_string();
        }
        select_statement.order_by(
            ColumnRef::Column(Name::Column(sort_col).into_iden()),
            Order::Asc,
        );

        Ok((select_statement, map))
    }

    pub fn create_query_nested(
        &self,
        tsq: &TimeseriesQuery,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        match tsq {
            TimeseriesQuery::Basic(b) => self.create_basic_select(b, project_date_partition),
            TimeseriesQuery::Filtered(tsq, filter) => {
                let (se, need_partition_columns) = self.create_filter_expressions(
                    filter,
                    Some(
                        &tsq.get_timestamp_variables()
                            .get(0)
                            .unwrap()
                            .variable
                            .as_str()
                            .to_string(),
                    ),
                )?;

                let (select, mut columns) = self
                    .create_query_nested(tsq, need_partition_columns || project_date_partition)?;

                let wraps_inner = if let TimeseriesQuery::Basic(_) = **tsq {
                    true
                } else {
                    false
                };
                let mut use_select;
                if wraps_inner || (!project_date_partition && need_partition_columns) {
                    let alias = "filtering_query";
                    let mut outer_select = Query::select();
                    outer_select.from_subquery(select, Alias::new(alias));
                    if !project_date_partition && need_partition_columns {
                        columns.remove(YEAR_PARTITION_COLUMN_NAME);
                        columns.remove(MONTH_PARTITION_COLUMN_NAME);
                        columns.remove(DAY_PARTITION_COLUMN_NAME);
                    }
                    let mut sorted_cols: Vec<&String> = columns.iter().collect();
                    sorted_cols.sort();
                    for c in sorted_cols {
                        outer_select.expr(SimpleExpr::Column(ColumnRef::Column(
                            Name::Column(c.clone()).into_iden(),
                        )));
                    }
                    use_select = outer_select;
                } else {
                    use_select = select;
                }

                use_select.and_where(se);

                Ok((use_select, columns))
            }
            TimeseriesQuery::InnerSynchronized(inner, synchronizers) => {
                if synchronizers.iter().all(|x| {
                    #[allow(irrefutable_let_patterns)]
                    if let Synchronizer::Identity(_) = x {
                        true
                    } else {
                        false
                    }
                }) {
                    let mut selects = vec![];
                    for s in inner {
                        selects.push(self.create_query_nested(s, self.partition_support)?);
                    }
                    let groupby_col = tsq.get_groupby_column().unwrap();
                    if let Some(Synchronizer::Identity(timestamp_col)) = &synchronizers.get(0) {
                        Ok(self.inner_join_selects(selects, timestamp_col, groupby_col))
                    } else {
                        panic!()
                    }
                } else {
                    todo!("Not implemented yet")
                }
            }
            TimeseriesQuery::Grouped(grouped) => self.create_grouped_query(
                &grouped.tsq,
                &grouped.by,
                &grouped.aggregations,
                project_date_partition,
            ),
            TimeseriesQuery::GroupedBasic(btsq, df, col) => {
                self.create_grouped_basic(btsq, project_date_partition, df, col)
            }
            TimeseriesQuery::ExpressionAs(tsq, v, e) => {
                self.create_expression_as(tsq, project_date_partition, v, e)
            }
        }
    }

    fn create_expression_as(
        &self,
        tsq: &TimeseriesQuery,
        project_date_partition: bool,
        v: &Variable,
        e: &Expression,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        let subquery_alias = "subquery";
        let subquery_name = Name::Table(subquery_alias.to_string());
        let mut expr_transformer =
            self.create_transformer(Some(&subquery_name), self.database_type.clone());
        let se = expr_transformer.sparql_expression_to_sql_expression(e)?;

        let (select, mut columns) = self.create_query_nested(
            tsq,
            project_date_partition || expr_transformer.used_partitioning,
        )?;
        if !project_date_partition && expr_transformer.used_partitioning {
            columns.remove(YEAR_PARTITION_COLUMN_NAME);
            columns.remove(MONTH_PARTITION_COLUMN_NAME);
            columns.remove(DAY_PARTITION_COLUMN_NAME);
        }

        let mut expression_select = Query::select();
        expression_select.from_subquery(select, Alias::new(subquery_alias));
        if !project_date_partition && expr_transformer.used_partitioning {
            columns.remove(YEAR_PARTITION_COLUMN_NAME);
            columns.remove(MONTH_PARTITION_COLUMN_NAME);
            columns.remove(DAY_PARTITION_COLUMN_NAME);
        }

        let mut sorted_cols: Vec<&String> = columns.iter().collect();
        sorted_cols.sort();
        for c in sorted_cols {
            expression_select.expr_as(
                SimpleExpr::Column(ColumnRef::Column(Name::Column(c.clone()).into_iden())),
                Alias::new(c),
            );
        }
        expression_select.expr_as(se, Alias::new(v.as_str()));
        columns.insert(v.as_str().to_string());
        Ok((expression_select, columns))
    }

    fn create_grouped_basic(
        &self,
        btsq: &BasicTimeseriesQuery,
        project_date_partition: bool,
        df: &DataFrame,
        column_name: &String,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        let mut value_tuples = vec![];
        let identifier_colname = btsq.identifier_variable.as_ref().unwrap().as_str();
        let mut identifier_iter = df.column(identifier_colname).unwrap().iter();
        let mut groupcol_iter = df.column(&column_name).unwrap().iter();
        for _ in 0..df.height() {
            let id = identifier_iter.next().unwrap();
            let grp = groupcol_iter.next().unwrap();
            let id_value = if let AnyValue::Utf8(id_value) = id {
                id_value.to_string()
            } else {
                panic!("Should never happen");
            };
            let grp_value = if let AnyValue::Int64(grp_value) = grp {
                grp_value
            } else {
                panic!("Should never happen");
            };
            value_tuples.push((id_value, grp_value));
        }

        let mut static_select = Query::select();
        match &self.database_type {
            DatabaseType::BigQuery => {
                static_select.columns([
                    ColumnRef::Column(Name::Column(identifier_colname.to_string()).into_iden()),
                    ColumnRef::Column(Name::Column(column_name.to_string()).into_iden()),
                ]);
                let mut structs = vec![];
                for (e, n) in value_tuples {
                    structs.push(SimpleExpr::Struct(vec![
                        NamedField::new(
                            Some(identifier_colname.to_string()),
                            SimpleExpr::Value(Value::String(Some(Box::new(e.to_string())))),
                        ),
                        NamedField::new(
                            Some(column_name.to_string()),
                            SimpleExpr::Value(Value::BigInt(Some(n))),
                        ),
                    ]))
                }
                static_select.from(TableRef::Unnest(
                    Unnest::new(structs),
                    Name::Table("values".to_string()).into_iden(),
                ));
            }
            _ => {
                panic!("Should never happen")
            }
        }

        let static_alias = "static_query";

        let (basic_select, mut columns) = self.create_basic_select(btsq, project_date_partition)?;

        let mut joined_select = Query::select();
        let basic_alias = "basic_query";
        joined_select.from_subquery(basic_select, Alias::new(basic_alias));

        joined_select.join(
            JoinType::InnerJoin,
            TableRef::SubQuery(
                static_select,
                Name::Table(static_alias.to_string()).into_iden(),
            ),
            SimpleExpr::Column(ColumnRef::TableColumn(
                Name::Table(static_alias.to_string()).into_iden(),
                Name::Column(identifier_colname.to_string()).into_iden(),
            ))
            .eq(SimpleExpr::Column(ColumnRef::TableColumn(
                Name::Table(basic_alias.to_string()).into_iden(),
                Name::Column(identifier_colname.to_string()).into_iden(),
            ))),
        );

        let mut sorted_cols: Vec<&String> = columns.iter().collect();
        sorted_cols.sort();
        for c in sorted_cols {
            if c != identifier_colname {
                joined_select.expr_as(
                    SimpleExpr::Column(ColumnRef::TableColumn(
                        Name::Table(basic_alias.to_string()).into_iden(),
                        Name::Column(c.clone()).into_iden(),
                    )),
                    Alias::new(c),
                );
            }
        }
        columns.remove(identifier_colname);

        joined_select.expr_as(
            SimpleExpr::Column(ColumnRef::TableColumn(
                Name::Table(static_alias.to_string()).into_iden(),
                Name::Column(column_name.to_string()).into_iden(),
            )),
            Alias::new(column_name),
        );
        columns.insert(column_name.to_string());

        Ok((joined_select, columns))
    }

    fn create_basic_select(
        &self,
        btsq: &BasicTimeseriesQuery,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        let table = self.find_right_table(btsq)?;
        let (select, columns) = table.create_basic_query(btsq, project_date_partition)?;

        Ok((select, columns))
    }

    fn inner_join_selects(
        &self,
        mut selects_and_timestamp_cols: Vec<(SelectStatement, HashSet<String>)>,
        timestamp_col: &String,
        groupby_col: &String,
    ) -> (SelectStatement, HashSet<String>) {
        let (mut first_select, mut first_columns) = selects_and_timestamp_cols.remove(0);
        let mut new_first_select = Query::select();
        let first_select_name = "first_query";
        new_first_select.from_subquery(first_select, Alias::new(first_select_name));
        let mut sorted_cols: Vec<&String> = first_columns.iter().collect();
        sorted_cols.sort();
        for c in sorted_cols {
            new_first_select.expr_as(
                SimpleExpr::Column(ColumnRef::TableColumn(
                    Name::Table(first_select_name.to_string()).into_iden(),
                    Name::Column(c.to_string()).into_iden(),
                )),
                Alias::new(c),
            );
        }
        first_select = new_first_select;

        for (i, (s, cols)) in selects_and_timestamp_cols.into_iter().enumerate() {
            let select_name = format!("other_{}", i);
            let mut conditions = vec![];

            let mut col_conditions = vec![
                groupby_col.clone(),
                timestamp_col.clone(),
            ];
            if self.partition_support {
                col_conditions.push(YEAR_PARTITION_COLUMN_NAME.to_string());
                col_conditions.push(MONTH_PARTITION_COLUMN_NAME.to_string());
                col_conditions.push(DAY_PARTITION_COLUMN_NAME.to_string());
            }
            for c in col_conditions {
                conditions.push(
                    SimpleExpr::Column(ColumnRef::TableColumn(
                        Name::Table(first_select_name.to_string()).into_iden(),
                        Name::Column(c.clone()).into_iden(),
                    ))
                    .eq(SimpleExpr::Column(ColumnRef::TableColumn(
                        Name::Table(select_name.clone()).into_iden(),
                        Name::Column(c).into_iden(),
                    ))),
                );
            }
            let mut first_condition = conditions.remove(0);
            for c in conditions {
                first_condition =
                    SimpleExpr::Binary(Box::new(first_condition), BinOper::And, Box::new(c));
            }

            first_select.join(
                JoinType::InnerJoin,
                TableRef::SubQuery(s, Alias::new(&select_name).into_iden()),
                first_condition,
            );
            let mut sorted_cols: Vec<&String> = cols.iter().collect();
            sorted_cols.sort();
            for c in sorted_cols {
                if c != timestamp_col {
                    first_select.expr_as(
                        SimpleExpr::Column(ColumnRef::TableColumn(
                            Name::Table(select_name.clone()).into_iden(),
                            Name::Column(c.clone()).into_iden(),
                        )),
                        Alias::new(c),
                    );
                    first_columns.insert(c.clone());
                }
            }
        }
        (first_select, first_columns)
    }

    fn find_right_table<'a>(
        &'a self,
        btsq: &BasicTimeseriesQuery,
    ) -> Result<&'a TimeseriesTable, TimeseriesQueryToSQLError> {
        if let Some(resource) = &btsq.resource {
            for table in self.tables {
                if &table.resource_name == resource {
                    return Ok(table);
                }
            }
            let alternatives = self
                .tables
                .iter()
                .map(|x| x.resource_name.clone())
                .collect();
            Err(TimeseriesQueryToSQLError::TimeseriesResourceNotFound(
                resource.clone(),
                alternatives,
            ))
        } else {
            Err(TimeseriesQueryToSQLError::MissingTimeseriesResource)
        }
    }

    fn create_filter_expressions(
        &self,
        expression: &Expression,
        timestamp_column: Option<&String>,
    ) -> Result<(SimpleExpr, bool), TimeseriesQueryToSQLError> {
        let mut transformer = self.create_transformer(None, self.database_type.clone());
        let mut se = transformer.sparql_expression_to_sql_expression(expression)?;
        let mut partitioned = false;
        if self.partition_support {
            let (se_part, part_status) = add_partitioned_timestamp_conditions(
                se,
                &timestamp_column.unwrap(),
                YEAR_PARTITION_COLUMN_NAME,
                MONTH_PARTITION_COLUMN_NAME,
                DAY_PARTITION_COLUMN_NAME,
            );
            se = se_part;
            partitioned = part_status || transformer.used_partitioning;
        }
        Ok((se, partitioned))
    }

    fn create_grouped_query(
        &self,
        inner_tsq: &TimeseriesQuery,
        by: &Vec<Variable>,
        aggregations: &Vec<(Variable, AggregateExpression)>,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        //Inner query timeseries functions:
        let inner_query_str = "inner_query";
        let inner_query_name = Name::Table(inner_query_str.to_string());

        //Outer query aggregations:
        let outer_query_str = "outer_query";
        let outer_query_name = Name::Table(outer_query_str.to_string());
        let mut new_columns = HashSet::new();
        let mut agg_transformer =
            self.create_transformer(Some(&outer_query_name), self.database_type.clone());
        let mut aggs = vec![];
        for (_, agg) in aggregations {
            aggs.push(agg_transformer.sparql_aggregate_expression_to_sql_expression(agg)?);
        }

        let (query, mut columns) = self.create_query_nested(
            &inner_tsq,
            agg_transformer.used_partitioning || project_date_partition,
        )?;
        if !project_date_partition && agg_transformer.used_partitioning {
            columns.remove(YEAR_PARTITION_COLUMN_NAME);
            columns.remove(MONTH_PARTITION_COLUMN_NAME);
            columns.remove(DAY_PARTITION_COLUMN_NAME);
        }
        let mut inner_query = Query::select();

        inner_query.from_subquery(query, inner_query_name.clone());
        let mut sorted_cols: Vec<&String> = columns.iter().collect();
        sorted_cols.sort();
        for c in sorted_cols {
            inner_query.expr_as(
                SimpleExpr::Column(ColumnRef::TableColumn(
                    inner_query_name.clone().into_iden(),
                    Name::Column(c.to_string()).into_iden(),
                )),
                Alias::new(c),
            );
        }

        let mut outer_query = Query::select();
        outer_query.from_subquery(inner_query, Alias::new(outer_query_str));

        for (v, _) in aggregations {
            let agg_trans = aggs.remove(0);
            outer_query.expr_as(agg_trans, Alias::new(v.as_str()));
            new_columns.insert(v.as_str().to_string());
        }

        outer_query.group_by_columns(
            by.iter()
                .map(|x| {
                    ColumnRef::TableColumn(
                        outer_query_name.clone().into_iden(),
                        Name::Column(x.as_str().to_string()).into_iden(),
                    )
                })
                .collect::<Vec<ColumnRef>>(),
        );
        for v in by {
            outer_query.expr_as(
                SimpleExpr::Column(ColumnRef::TableColumn(
                    outer_query_name.clone().into_iden(),
                    Name::Column(v.as_str().to_string()).into_iden(),
                )),
                Alias::new(v.as_str()),
            );
            new_columns.insert(v.as_str().to_string());
        }
        Ok((outer_query, new_columns))
    }

    fn create_transformer<'a>(
        &'a self,
        table_name: Option<&'a Name>,
        database_type: DatabaseType,
    ) -> SPARQLToSQLExpressionTransformer {
        if self.partition_support {
            SPARQLToSQLExpressionTransformer::new(
                table_name,
                Some(YEAR_PARTITION_COLUMN_NAME),
                Some(MONTH_PARTITION_COLUMN_NAME),
                Some(DAY_PARTITION_COLUMN_NAME),
                database_type,
            )
        } else {
            SPARQLToSQLExpressionTransformer::new(table_name, None, None, None, database_type)
        }
    }
}

impl TimeseriesTable {
    pub fn create_basic_query(
        &self,
        btsq: &BasicTimeseriesQuery,
        project_date_partition: bool,
    ) -> Result<(SelectStatement, HashSet<String>), TimeseriesQueryToSQLError> {
        let mut basic_query = Query::select();
        let mut variable_column_name_map = HashMap::new();
        variable_column_name_map.insert(
            btsq.identifier_variable
                .as_ref()
                .unwrap()
                .as_str()
                .to_string(),
            self.identifier_column.clone(),
        );
        variable_column_name_map.insert(
            btsq.value_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.value_column.clone(),
        );
        variable_column_name_map.insert(
            btsq.timestamp_variable
                .as_ref()
                .unwrap()
                .variable
                .as_str()
                .to_string(),
            self.timestamp_column.clone(),
        );
        let mut projection_column_name_map = HashMap::new();
        if project_date_partition {
            projection_column_name_map.insert(
                YEAR_PARTITION_COLUMN_NAME.to_string(),
                self.year_column.as_ref().unwrap().clone(),
            );
            projection_column_name_map.insert(
                MONTH_PARTITION_COLUMN_NAME.to_string(),
                self.month_column.as_ref().unwrap().clone(),
            );
            projection_column_name_map.insert(
                DAY_PARTITION_COLUMN_NAME.to_string(),
                self.day_column.as_ref().unwrap().clone(),
            );
        }
        let mut columns = HashSet::new();

        let mut kvs: Vec<_> = variable_column_name_map.iter().collect();
        kvs.sort();
        for (k, v) in kvs {
            basic_query.expr_as(SeaExpr::col(Name::Column(v.clone())), Alias::new(k));
            columns.insert(k.clone());
        }

        let mut kvs: Vec<_> = projection_column_name_map.iter().collect();
        kvs.sort();
        for (k, v) in kvs {
            basic_query.expr_as(
                SeaExpr::col(Name::Column(v.clone())).as_enum(Alias::new("INTEGER")),
                Alias::new(k),
            );
            columns.insert(k.clone());
        }

        if let Some(schema) = &self.schema {
            basic_query.from((
                Name::Schema(schema.clone()),
                Name::Table(self.time_series_table.clone()),
            ));
        } else {
            basic_query.from(Name::Table(self.time_series_table.clone()));
        }

        if let Some(ids) = &btsq.ids {
            basic_query.and_where(
                SeaExpr::col(Name::Column(self.identifier_column.clone())).is_in(
                    ids.iter()
                        .map(|x| Value::String(Some(Box::new(x.to_string())))),
                ),
            );
        }

        Ok((basic_query, columns))
    }
}

fn check_partitioning_support(tables: &Vec<TimeseriesTable>) -> bool {
    tables
        .iter()
        .all(|x| x.day_column.is_some() && x.month_column.is_some() && x.day_column.is_some())
}

#[cfg(test)]
mod tests {
    use representation::query_context::{Context, VariableInContext};
    use crate::timeseries_database::timeseries_sql_rewrite::{
        TimeseriesQueryToSQLTransformer, TimeseriesTable,
    };
    use crate::timeseries_database::DatabaseType;
    use crate::timeseries_query::{
        BasicTimeseriesQuery, GroupedTimeseriesQuery, Synchronizer, TimeseriesQuery,
    };
    use oxrdf::vocab::xsd;
    use oxrdf::{Literal, Variable};
    use polars_core::frame::DataFrame;
    use polars_core::prelude::NamedFrom;
    use polars_core::series::Series;
    use sea_query::BigQueryQueryBuilder;
    use spargebra::algebra::{AggregateExpression, Expression, Function};
    use std::vec;

    #[test]
    pub fn test_translate() {
        let basic_tsq = BasicTimeseriesQuery {
            identifier_variable: Some(Variable::new_unchecked("id")),
            timeseries_variable: Some(VariableInContext::new(
                Variable::new_unchecked("ts"),
                Context::new(),
            )),
            data_point_variable: Some(VariableInContext::new(
                Variable::new_unchecked("dp"),
                Context::new(),
            )),
            value_variable: Some(VariableInContext::new(
                Variable::new_unchecked("v"),
                Context::new(),
            )),
            resource_variable: Some(Variable::new_unchecked("res")),
            resource: Some("my_resource".to_string()),
            timestamp_variable: Some(VariableInContext::new(
                Variable::new_unchecked("t"),
                Context::new(),
            )),
            ids: Some(vec!["A".to_string(), "B".to_string()]),
        };
        let tsq = TimeseriesQuery::Filtered(
            Box::new(TimeseriesQuery::Basic(basic_tsq)),
            Expression::LessOrEqual(
                Box::new(Expression::Variable(Variable::new_unchecked("t"))),
                Box::new(Expression::Literal(Literal::new_typed_literal(
                    "2022-06-01T08:46:53",
                    xsd::DATE_TIME,
                ))),
            ),
        );

        let table = TimeseriesTable {
            resource_name: "my_resource".into(),
            schema: Some("s3.ct-benchmark".into()),
            time_series_table: "timeseries_double".into(),
            value_column: "value".into(),
            timestamp_column: "timestamp".into(),
            identifier_column: "dir3".into(),
            year_column: Some("dir0".to_string()),
            month_column: Some("dir1".to_string()),
            day_column: Some("dir2".to_string()),
        };
        let tables = vec![table];
        let transformer = TimeseriesQueryToSQLTransformer::new(&tables, DatabaseType::BigQuery);
        let (sql_query, _) = transformer.create_query(&tsq, false).unwrap();
        assert_eq!(
            &sql_query.to_string(BigQueryQueryBuilder),
            r#"SELECT `id`, `t`, `v` FROM (SELECT `dir3` AS `id`, `timestamp` AS `t`, `value` AS `v`, CAST(`dir2` AS INTEGER) AS `day_partition_column_name`, CAST(`dir1` AS INTEGER) AS `month_partition_column_name`, CAST(`dir0` AS INTEGER) AS `year_partition_column_name` FROM `s3.ct-benchmark`.`timeseries_double` WHERE `dir3` IN ('A', 'B')) AS `filtering_query` WHERE `year_partition_column_name` < 2022 OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` < 6) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 6 AND `day_partition_column_name` < 1) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 6 AND `day_partition_column_name` = 1 AND `t` <= '2022-06-01 08:46:53') ORDER BY `id` ASC"#
        );
    }

    #[test]
    fn test_synchronized_grouped() {
        let tsq = TimeseriesQuery::Grouped(GroupedTimeseriesQuery {
            tsq: Box::new(TimeseriesQuery::ExpressionAs(
                Box::new(TimeseriesQuery::ExpressionAs(
                    Box::new(TimeseriesQuery::ExpressionAs(
                        Box::new(TimeseriesQuery::ExpressionAs(
                            Box::new(TimeseriesQuery::ExpressionAs(
                                Box::new(TimeseriesQuery::Filtered(
                                    Box::new(TimeseriesQuery::InnerSynchronized(
                                        vec![
                                            Box::new(TimeseriesQuery::GroupedBasic(
                                                BasicTimeseriesQuery {
                                                    identifier_variable: Some(
                                                        Variable::new_unchecked("ts_external_id_1"),
                                                    ),
                                                    timeseries_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("ts_speed"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    data_point_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("dp_speed"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    value_variable: Some(VariableInContext::new(
                                                        Variable::new_unchecked("val_speed"),
                                                        Context::new(),
                                                    )),
                                                    resource_variable: Some(
                                                        Variable::new_unchecked("ts_resource_1"),
                                                    ),
                                                    resource: Some("my_resource".into()),
                                                    timestamp_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("t"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    ids: Some(vec!["id1".to_string()]),
                                                },
                                                DataFrame::new(vec![
                                                    Series::new("ts_external_id_1", ["id1"]),
                                                    Series::new("grouping_col_0", [0i64]),
                                                ])
                                                .unwrap(),
                                                "grouping_col_0".to_string(),
                                            )),
                                            Box::new(TimeseriesQuery::GroupedBasic(
                                                BasicTimeseriesQuery {
                                                    identifier_variable: Some(
                                                        Variable::new_unchecked("ts_external_id_2"),
                                                    ),
                                                    timeseries_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("ts_dir"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    data_point_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("dp_dir"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    value_variable: Some(VariableInContext::new(
                                                        Variable::new_unchecked("val_dir"),
                                                        Context::new(),
                                                    )),
                                                    resource_variable: Some(
                                                        Variable::new_unchecked("ts_resource_2"),
                                                    ),
                                                    resource: Some("my_resource".into()),
                                                    timestamp_variable: Some(
                                                        VariableInContext::new(
                                                            Variable::new_unchecked("t"),
                                                            Context::new(),
                                                        ),
                                                    ),
                                                    ids: Some(vec!["id2".to_string()]),
                                                },
                                                DataFrame::new(vec![
                                                    Series::new("ts_external_id_2", ["id2"]),
                                                    Series::new("grouping_col_0", [1i64]),
                                                ])
                                                .unwrap(),
                                                "grouping_col_0".to_string(),
                                            )),
                                        ],
                                        vec![Synchronizer::Identity("t".to_string())],
                                    )),
                                    Expression::And(
                                        Box::new(Expression::GreaterOrEqual(
                                            Box::new(Expression::Variable(
                                                Variable::new_unchecked("t"),
                                            )),
                                            Box::new(Expression::Literal(
                                                Literal::new_typed_literal(
                                                    "2022-08-30T08:46:53",
                                                    xsd::DATE_TIME,
                                                ),
                                            )),
                                        )),
                                        Box::new(Expression::LessOrEqual(
                                            Box::new(Expression::Variable(
                                                Variable::new_unchecked("t"),
                                            )),
                                            Box::new(Expression::Literal(
                                                Literal::new_typed_literal(
                                                    "2022-08-30T21:46:53",
                                                    xsd::DATE_TIME,
                                                ),
                                            )),
                                        )),
                                    ),
                                )),
                                Variable::new_unchecked("minute_10"),
                                Expression::FunctionCall(
                                    Function::Custom(xsd::INTEGER.into_owned()),
                                    vec![Expression::FunctionCall(
                                        Function::Floor,
                                        vec![Expression::Divide(
                                            Box::new(Expression::FunctionCall(
                                                Function::Minutes,
                                                vec![Expression::Variable(
                                                    Variable::new_unchecked("t"),
                                                )],
                                            )),
                                            Box::new(Expression::Literal(
                                                Literal::new_typed_literal("10.0", xsd::DECIMAL),
                                            )),
                                        )],
                                    )],
                                ),
                            )),
                            Variable::new_unchecked("hour"),
                            Expression::FunctionCall(
                                Function::Hours,
                                vec![Expression::Variable(Variable::new_unchecked("t"))],
                            ),
                        )),
                        Variable::new_unchecked("day"),
                        Expression::FunctionCall(
                            Function::Day,
                            vec![Expression::Variable(Variable::new_unchecked("t"))],
                        ),
                    )),
                    Variable::new_unchecked("month"),
                    Expression::FunctionCall(
                        Function::Month,
                        vec![Expression::Variable(Variable::new_unchecked("t"))],
                    ),
                )),
                Variable::new_unchecked("year"),
                Expression::FunctionCall(
                    Function::Year,
                    vec![Expression::Variable(Variable::new_unchecked("t"))],
                ),
            )),

            context: Context::new(),
            by: vec![
                Variable::new_unchecked("year".to_string()),
                Variable::new_unchecked("month".to_string()),
                Variable::new_unchecked("day".to_string()),
                Variable::new_unchecked("hour".to_string()),
                Variable::new_unchecked("minute_10"),
                Variable::new_unchecked("grouping_col_0"),
            ],
            aggregations: vec![
                (
                    Variable::new_unchecked("f7ca5ee9058effba8691ac9c642fbe95"),
                    AggregateExpression::Avg {
                        expr: Box::new(Expression::Variable(Variable::new_unchecked("val_dir"))),
                        distinct: false,
                    },
                ),
                (
                    Variable::new_unchecked("990362f372e4019bc151c13baf0b50d5"),
                    AggregateExpression::Avg {
                        expr: Box::new(Expression::Variable(Variable::new_unchecked("val_speed"))),
                        distinct: false,
                    },
                ),
            ],
        });

        let table = TimeseriesTable {
            resource_name: "my_resource".to_string(),
            schema: Some("s3.ct-benchmark".into()),
            time_series_table: "timeseries_double".into(),
            value_column: "value".into(),
            timestamp_column: "timestamp".into(),
            identifier_column: "dir3".into(),
            year_column: Some("dir0".to_string()),
            month_column: Some("dir1".to_string()),
            day_column: Some("dir2".to_string()),
        };
        let tables = vec![table];
        let transformer = TimeseriesQueryToSQLTransformer::new(&tables, DatabaseType::BigQuery);
        let (sql_query, _) = transformer.create_query(&tsq, false).unwrap();

        let expected_str = r#"SELECT AVG(`outer_query`.`val_dir`) AS `f7ca5ee9058effba8691ac9c642fbe95`, AVG(`outer_query`.`val_speed`) AS `990362f372e4019bc151c13baf0b50d5`, `outer_query`.`year` AS `year`, `outer_query`.`month` AS `month`, `outer_query`.`day` AS `day`, `outer_query`.`hour` AS `hour`, `outer_query`.`minute_10` AS `minute_10`, `outer_query`.`grouping_col_0` AS `grouping_col_0` FROM (SELECT `inner_query`.`day` AS `day`, `inner_query`.`grouping_col_0` AS `grouping_col_0`, `inner_query`.`hour` AS `hour`, `inner_query`.`minute_10` AS `minute_10`, `inner_query`.`month` AS `month`, `inner_query`.`t` AS `t`, `inner_query`.`val_dir` AS `val_dir`, `inner_query`.`val_speed` AS `val_speed`, `inner_query`.`year` AS `year` FROM (SELECT `day` AS `day`, `grouping_col_0` AS `grouping_col_0`, `hour` AS `hour`, `minute_10` AS `minute_10`, `month` AS `month`, `t` AS `t`, `val_dir` AS `val_dir`, `val_speed` AS `val_speed`, `subquery`.`year_partition_column_name` AS `year` FROM (SELECT `day` AS `day`, `day_partition_column_name` AS `day_partition_column_name`, `grouping_col_0` AS `grouping_col_0`, `hour` AS `hour`, `minute_10` AS `minute_10`, `month_partition_column_name` AS `month_partition_column_name`, `t` AS `t`, `val_dir` AS `val_dir`, `val_speed` AS `val_speed`, `year_partition_column_name` AS `year_partition_column_name`, `subquery`.`month_partition_column_name` AS `month` FROM (SELECT `day_partition_column_name` AS `day_partition_column_name`, `grouping_col_0` AS `grouping_col_0`, `hour` AS `hour`, `minute_10` AS `minute_10`, `month_partition_column_name` AS `month_partition_column_name`, `t` AS `t`, `val_dir` AS `val_dir`, `val_speed` AS `val_speed`, `year_partition_column_name` AS `year_partition_column_name`, `subquery`.`day_partition_column_name` AS `day` FROM (SELECT `day_partition_column_name` AS `day_partition_column_name`, `grouping_col_0` AS `grouping_col_0`, `minute_10` AS `minute_10`, `month_partition_column_name` AS `month_partition_column_name`, `t` AS `t`, `val_dir` AS `val_dir`, `val_speed` AS `val_speed`, `year_partition_column_name` AS `year_partition_column_name`, EXTRACT(HOUR FROM `subquery`.`t`) AS `hour` FROM (SELECT `day_partition_column_name` AS `day_partition_column_name`, `grouping_col_0` AS `grouping_col_0`, `month_partition_column_name` AS `month_partition_column_name`, `t` AS `t`, `val_dir` AS `val_dir`, `val_speed` AS `val_speed`, `year_partition_column_name` AS `year_partition_column_name`, CAST(FLOOR(EXTRACT(MINUTE FROM `subquery`.`t`) / 10) AS INTEGER) AS `minute_10` FROM (SELECT `first_query`.`day_partition_column_name` AS `day_partition_column_name`, `first_query`.`grouping_col_0` AS `grouping_col_0`, `first_query`.`month_partition_column_name` AS `month_partition_column_name`, `first_query`.`t` AS `t`, `first_query`.`val_speed` AS `val_speed`, `first_query`.`year_partition_column_name` AS `year_partition_column_name`, `other_0`.`day_partition_column_name` AS `day_partition_column_name`, `other_0`.`grouping_col_0` AS `grouping_col_0`, `other_0`.`month_partition_column_name` AS `month_partition_column_name`, `other_0`.`val_dir` AS `val_dir`, `other_0`.`year_partition_column_name` AS `year_partition_column_name` FROM (SELECT `basic_query`.`day_partition_column_name` AS `day_partition_column_name`, `basic_query`.`month_partition_column_name` AS `month_partition_column_name`, `basic_query`.`t` AS `t`, `basic_query`.`val_speed` AS `val_speed`, `basic_query`.`year_partition_column_name` AS `year_partition_column_name`, `static_query`.`grouping_col_0` AS `grouping_col_0` FROM (SELECT `timestamp` AS `t`, `dir3` AS `ts_external_id_1`, `value` AS `val_speed`, CAST(`dir2` AS INTEGER) AS `day_partition_column_name`, CAST(`dir1` AS INTEGER) AS `month_partition_column_name`, CAST(`dir0` AS INTEGER) AS `year_partition_column_name` FROM `s3.ct-benchmark`.`timeseries_double` WHERE `dir3` IN ('id1')) AS `basic_query` INNER JOIN (SELECT `ts_external_id_1`, `grouping_col_0` FROM UNNEST([STRUCT ('id1' AS ts_external_id_1,0 AS grouping_col_0)]) AS `values`) AS `static_query` ON `static_query`.`ts_external_id_1` = `basic_query`.`ts_external_id_1`) AS `first_query` INNER JOIN (SELECT `basic_query`.`day_partition_column_name` AS `day_partition_column_name`, `basic_query`.`month_partition_column_name` AS `month_partition_column_name`, `basic_query`.`t` AS `t`, `basic_query`.`val_dir` AS `val_dir`, `basic_query`.`year_partition_column_name` AS `year_partition_column_name`, `static_query`.`grouping_col_0` AS `grouping_col_0` FROM (SELECT `timestamp` AS `t`, `dir3` AS `ts_external_id_2`, `value` AS `val_dir`, CAST(`dir2` AS INTEGER) AS `day_partition_column_name`, CAST(`dir1` AS INTEGER) AS `month_partition_column_name`, CAST(`dir0` AS INTEGER) AS `year_partition_column_name` FROM `s3.ct-benchmark`.`timeseries_double` WHERE `dir3` IN ('id2')) AS `basic_query` INNER JOIN (SELECT `ts_external_id_2`, `grouping_col_0` FROM UNNEST([STRUCT ('id2' AS ts_external_id_2,1 AS grouping_col_0)]) AS `values`) AS `static_query` ON `static_query`.`ts_external_id_2` = `basic_query`.`ts_external_id_2`) AS `other_0` ON `first_query`.`grouping_col_0` = `other_0`.`grouping_col_0` AND `first_query`.`t` = `other_0`.`t` AND `first_query`.`year_partition_column_name` = `other_0`.`year_partition_column_name` AND `first_query`.`month_partition_column_name` = `other_0`.`month_partition_column_name` AND `first_query`.`day_partition_column_name` = `other_0`.`day_partition_column_name` WHERE (`year_partition_column_name` > 2022 OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` > 8) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 8 AND `day_partition_column_name` > 30) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 8 AND `day_partition_column_name` = 30 AND `t` >= '2022-08-30 08:46:53')) AND (`year_partition_column_name` < 2022 OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` < 8) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 8 AND `day_partition_column_name` < 30) OR (`year_partition_column_name` = 2022 AND `month_partition_column_name` = 8 AND `day_partition_column_name` = 30 AND `t` <= '2022-08-30 21:46:53'))) AS `subquery`) AS `subquery`) AS `subquery`) AS `subquery`) AS `subquery`) AS `inner_query`) AS `outer_query` GROUP BY `outer_query`.`year`, `outer_query`.`month`, `outer_query`.`day`, `outer_query`.`hour`, `outer_query`.`minute_10`, `outer_query`.`grouping_col_0` ORDER BY `grouping_col_0` ASC"#;
        assert_eq!(sql_query.to_string(BigQueryQueryBuilder), expected_str);
    }
}
