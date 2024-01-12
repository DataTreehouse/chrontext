use oxrdf::vocab::xsd;
use polars::export::chrono::{DateTime, NaiveDateTime, Utc};
use sea_query::extension::bigquery::{BqFunc, DateTimePart};
use sea_query::IntoIden;
use sea_query::{BinOper, ColumnRef, SimpleExpr, UnOper, Value};
use sea_query::{Expr as SeaExpr, Func};
use spargebra::algebra::Expression;

use query_processing::constants::{
    DATETIME_AS_SECONDS, FLOOR_DATETIME_TO_SECONDS_INTERVAL, SECONDS_AS_DATETIME,
};
use crate::timeseries_database::timeseries_sql_rewrite::{Name, TimeseriesQueryToSQLError};
use crate::timeseries_database::DatabaseType;

pub mod aggregate_expressions;

pub(crate) struct SPARQLToSQLExpressionTransformer<'a> {
    table_name: Option<&'a Name>,
    year_col: Option<&'a str>,
    month_col: Option<&'a str>,
    day_col: Option<&'a str>,
    pub used_partitioning: bool,
    database_type: DatabaseType,
}

impl SPARQLToSQLExpressionTransformer<'_> {
    pub(crate) fn new<'a>(
        table_name: Option<&'a Name>,
        year_col: Option<&'a str>,
        month_col: Option<&'a str>,
        day_col: Option<&'a str>,
        database_type: DatabaseType,
    ) -> SPARQLToSQLExpressionTransformer<'a> {
        SPARQLToSQLExpressionTransformer {
            table_name,
            year_col,
            month_col,
            day_col,
            used_partitioning: false,
            database_type,
        }
    }

    pub(crate) fn sparql_expression_to_sql_expression(
        &mut self,
        e: &Expression,
    ) -> Result<SimpleExpr, TimeseriesQueryToSQLError> {
        Ok(match e {
            Expression::Or(left, right) => self
                .sparql_expression_to_sql_expression(left)?
                .or(self.sparql_expression_to_sql_expression(right)?),
            Expression::Literal(l) => {
                let v = l.value();
                let value = match l.datatype() {
                    xsd::BOOLEAN => Value::Bool(Some(v.parse().unwrap())),
                    xsd::DOUBLE => Value::Double(Some(v.parse().unwrap())),
                    xsd::DECIMAL => Value::Double(Some(v.parse().unwrap())),
                    xsd::FLOAT => Value::Float(Some(v.parse().unwrap())),
                    xsd::INTEGER => Value::BigInt(Some(v.parse().unwrap())),
                    xsd::LONG => Value::BigInt(Some(v.parse().unwrap())),
                    xsd::INT => Value::Int(Some(v.parse().unwrap())),
                    xsd::UNSIGNED_INT => Value::Unsigned(Some(v.parse().unwrap())),
                    xsd::UNSIGNED_LONG => Value::BigUnsigned(Some(v.parse().unwrap())),
                    xsd::STRING => Value::String(Some(Box::new(v.to_string()))),
                    xsd::DATE_TIME => {
                        if let Ok(dt) = v.parse::<NaiveDateTime>() {
                            Value::ChronoDateTime(Some(Box::new(dt)))
                        } else if let Ok(dt) = v.parse::<DateTime<Utc>>() {
                            Value::ChronoDateTimeUtc(Some(Box::new(dt)))
                        } else {
                            todo!("Could not parse {}", v);
                        }
                    }
                    _ => {
                        return Err(TimeseriesQueryToSQLError::UnknownDatatype(
                            l.datatype().as_str().to_string(),
                        ));
                    }
                };
                SimpleExpr::Value(value)
            }
            Expression::Variable(v) => simple_expr_from_column_name(&self.table_name, v.as_str()),
            Expression::And(left, right) => self
                .sparql_expression_to_sql_expression(left)?
                .and(self.sparql_expression_to_sql_expression(right)?),
            Expression::Equal(left, right) => self
                .sparql_expression_to_sql_expression(left)?
                .eq(self.sparql_expression_to_sql_expression(right)?),
            Expression::Greater(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(left)?),
                BinOper::GreaterThan,
                Box::new(self.sparql_expression_to_sql_expression(right)?),
            ),
            Expression::GreaterOrEqual(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(left)?),
                BinOper::GreaterThanOrEqual,
                Box::new(self.sparql_expression_to_sql_expression(right)?),
            ),
            Expression::Less(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(left)?),
                BinOper::SmallerThan,
                Box::new(self.sparql_expression_to_sql_expression(right)?),
            ),
            Expression::LessOrEqual(left, right) => {
                SimpleExpr::Binary(
                    Box::new(self.sparql_expression_to_sql_expression(left)?),
                    BinOper::SmallerThanOrEqual,
                    Box::new(self.sparql_expression_to_sql_expression(right)?),
                ) //Note flipped directions
            }
            Expression::In(left, right) => {
                let simple_right = right
                    .iter()
                    .map(|x| self.sparql_expression_to_sql_expression(x));
                let mut simple_right_values = vec![];
                for v in simple_right {
                    if let Ok(SimpleExpr::Value(v)) = v {
                        simple_right_values.push(v);
                    } else if let Err(e) = v {
                        return Err(e);
                    } else {
                        return Err(TimeseriesQueryToSQLError::FoundNonValueInInExpression);
                    }
                }
                SeaExpr::expr(self.sparql_expression_to_sql_expression(left)?)
                    .is_in(simple_right_values)
            }
            Expression::Add(left, right) => self
                .sparql_expression_to_sql_expression(left)?
                .add(self.sparql_expression_to_sql_expression(right)?),
            Expression::Subtract(left, right) => self
                .sparql_expression_to_sql_expression(left)?
                .sub(self.sparql_expression_to_sql_expression(right)?),
            Expression::Multiply(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(left)?),
                BinOper::Mul,
                Box::new(self.sparql_expression_to_sql_expression(right)?),
            ),
            Expression::Divide(left, right) => SimpleExpr::Binary(
                Box::new(self.sparql_expression_to_sql_expression(left)?),
                BinOper::Div,
                Box::new(self.sparql_expression_to_sql_expression(right)?),
            ),
            Expression::UnaryPlus(inner) => self.sparql_expression_to_sql_expression(inner)?,
            Expression::UnaryMinus(inner) => SimpleExpr::Value(Value::Double(Some(0.0)))
                .sub(self.sparql_expression_to_sql_expression(inner)?),
            Expression::Not(inner) => SimpleExpr::Unary(
                UnOper::Not,
                Box::new(self.sparql_expression_to_sql_expression(inner)?),
            ),
            Expression::FunctionCall(f, expressions) => match f {
                spargebra::algebra::Function::Floor => {
                    let e = expressions.first().unwrap();
                    let mapped_e = self.sparql_expression_to_sql_expression(e)?;
                    SimpleExpr::FunctionCall(
                        Func::cust(Name::Function("FLOOR".to_string()).into_iden())
                            .args(vec![mapped_e]),
                    )
                }
                spargebra::algebra::Function::Year
                | spargebra::algebra::Function::Month
                | spargebra::algebra::Function::Day
                | spargebra::algebra::Function::Hours
                | spargebra::algebra::Function::Minutes
                | spargebra::algebra::Function::Seconds => {
                    let e = expressions.first().unwrap();
                    let mapped_e = self.sparql_expression_to_sql_expression(e)?;
                    if f == &spargebra::algebra::Function::Year && self.year_col.is_some() {
                        self.used_partitioning = true;
                        simple_expr_from_column_name(
                            &self.table_name,
                            self.year_col.as_ref().unwrap(),
                        )
                    } else if f == &spargebra::algebra::Function::Month && self.month_col.is_some()
                    {
                        self.used_partitioning = true;
                        simple_expr_from_column_name(
                            &self.table_name,
                            self.month_col.as_ref().unwrap(),
                        )
                    } else if f == &spargebra::algebra::Function::Day && self.day_col.is_some() {
                        self.used_partitioning = true;
                        simple_expr_from_column_name(
                            &self.table_name,
                            self.day_col.as_ref().unwrap(),
                        )
                    } else {
                        SimpleExpr::FunctionCall(match &self.database_type {
                            DatabaseType::BigQuery => {
                                let datetime_part = match f {
                                    spargebra::algebra::Function::Year => DateTimePart::YEAR,
                                    spargebra::algebra::Function::Month => DateTimePart::MONTH,
                                    spargebra::algebra::Function::Day => DateTimePart::DAY,
                                    spargebra::algebra::Function::Hours => DateTimePart::HOUR,
                                    spargebra::algebra::Function::Minutes => DateTimePart::MINUTE,
                                    spargebra::algebra::Function::Seconds => DateTimePart::SECOND,
                                    _ => {
                                        panic!("Cannot happen")
                                    }
                                };
                                BqFunc::extract(datetime_part, mapped_e)
                            }
                            _ => {
                                panic!("Should never happen")
                            }
                        })
                    }
                }
                spargebra::algebra::Function::Custom(c) => {
                    let e1 = expressions.first().unwrap();
                    let mapped_e1 = self.sparql_expression_to_sql_expression(e1)?;
                    if c.as_str() == DATETIME_AS_SECONDS {
                        match self.database_type {
                            DatabaseType::BigQuery => SimpleExpr::FunctionCall(
                                Func::cust(Name::Function("UNIX_SECONDS".to_string()).into_iden())
                                    .args(vec![mapped_e1]),
                            ),
                            _ => {
                                panic!("Should never happen")
                            }
                        }
                    } else if c.as_str() == SECONDS_AS_DATETIME {
                        match self.database_type {
                            DatabaseType::BigQuery => SimpleExpr::FunctionCall(
                                Func::cust(
                                    Name::Function("TIMESTAMP_SECONDS".to_string()).into_iden(),
                                )
                                .args(vec![mapped_e1]),
                            ),
                            _ => {
                                unimplemented!()
                            }
                        }
                    } else if c.as_str() == FLOOR_DATETIME_TO_SECONDS_INTERVAL {
                        match self.database_type {
                            DatabaseType::BigQuery => {
                                let first_as_seconds = SimpleExpr::FunctionCall(
                                    Func::cust(
                                        Name::Function("UNIX_SECONDS".to_string()).into_iden(),
                                    )
                                    .args(vec![mapped_e1]),
                                );
                                let e2 = expressions.get(1).unwrap();
                                let mapped_e2 = self.sparql_expression_to_sql_expression(e2)?;
                                let first_as_seconds_modulus = SimpleExpr::FunctionCall(
                                    Func::cust(Name::Function("MOD".to_string()).into_iden())
                                        .args(vec![first_as_seconds.clone(), mapped_e2]),
                                );
                                SimpleExpr::FunctionCall(
                                    Func::cust(
                                        Name::Function("TIMESTAMP_SECONDS".to_string()).into_iden(),
                                    )
                                    .args(vec![
                                        SimpleExpr::Binary(
                                            Box::new(first_as_seconds.clone()),
                                            BinOper::Sub,
                                            Box::new(first_as_seconds_modulus),
                                        ),
                                    ]),
                                )
                            }
                            _ => {
                                unimplemented!()
                            }
                        }
                    } else if c.as_str() == xsd::INTEGER.as_str() {
                        SimpleExpr::AsEnum(
                            Name::Table("INTEGER".to_string()).into_iden(),
                            Box::new(mapped_e1),
                        )
                    } else {
                        todo!("Fix custom {}", c)
                    }
                }
                _ => {
                    todo!("{}", f)
                }
            },
            _ => {
                unimplemented!("")
            }
        })
    }
}

fn simple_expr_from_column_name(table_name: &Option<&Name>, column_name: &str) -> SimpleExpr {
    if let Some(name) = table_name {
        SimpleExpr::Column(ColumnRef::TableColumn(
            (*name).clone().into_iden(),
            Name::Column(column_name.to_string()).into_iden(),
        ))
    } else {
        SimpleExpr::Column(ColumnRef::Column(
            Name::Column(column_name.to_string()).into_iden(),
        ))
    }
}
