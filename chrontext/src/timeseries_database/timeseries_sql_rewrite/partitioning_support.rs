use super::Name;
use log::debug;
use polars_core::export::chrono::Datelike;
use sea_query::{BinOper, ColumnRef, FunctionCall, IntoIden, SimpleExpr, Value};

pub fn add_partitioned_timestamp_conditions(
    se: SimpleExpr,
    timestamp_col: &str,
    year_col: &str,
    month_col: &str,
    day_col: &str,
) -> (SimpleExpr, bool) {
    match se {
        SimpleExpr::Unary(op, inner) => {
            let (inner_rewrite, added) = add_partitioned_timestamp_conditions(
                *inner,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            );
            (
                SimpleExpr::Unary(op.clone(), Box::new(inner_rewrite)),
                added,
            )
        }
        SimpleExpr::FunctionCall(func_call) => {
            let inner_args = func_call.get_args();
            let rewrites_and_added: Vec<(SimpleExpr, bool)> = inner_args
                .into_iter()
                .map(|x| {
                    add_partitioned_timestamp_conditions(
                        x.clone(),
                        timestamp_col,
                        year_col,
                        month_col,
                        day_col,
                    )
                })
                .collect();
            let added = rewrites_and_added.iter().fold(false, |x, (_, y)| x || *y);
            let se_rewrites: Vec<SimpleExpr> =
                rewrites_and_added.into_iter().map(|(x, _)| x).collect();
            (
                SimpleExpr::FunctionCall(
                    FunctionCall::new(func_call.get_func().clone()).args(se_rewrites),
                ),
                added,
            )
        }
        SimpleExpr::Binary(left, op, right) => rewrite_binary_expression(
            *left,
            op,
            *right,
            timestamp_col,
            year_col,
            month_col,
            day_col,
        ),
        _ => (se, false),
    }
}

fn rewrite_binary_expression(
    left: SimpleExpr,
    op: BinOper,
    right: SimpleExpr,
    timestamp_col: &str,
    year_col: &str,
    month_col: &str,
    day_col: &str,
) -> (SimpleExpr, bool) {
    let original = SimpleExpr::Binary(Box::new(left.clone()), op, Box::new(right.clone()));
    match op {
        BinOper::In => {
            debug!("Binary in expression partition rewriting not supported yet")
        }
        BinOper::NotIn => {
            debug!("Binary not_in expression partition rewriting not supported yet")
        }
        BinOper::Equal => {
            if let Some(e) = oper_or_original(
                &original,
                &left,
                &right,
                BinOper::NotEqual,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        BinOper::NotEqual => {
            if let Some(e) = oper_or_original(
                &original,
                &left,
                &right,
                BinOper::NotEqual,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        BinOper::SmallerThan => {
            if let Some(e) = smaller_than_or_original(
                &original,
                &left,
                &right,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        BinOper::GreaterThan => {
            if let Some(e) = greater_than_or_original(
                &original,
                &left,
                &right,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        BinOper::SmallerThanOrEqual => {
            if let Some(e) = smaller_than_or_original(
                &original,
                &left,
                &right,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        BinOper::GreaterThanOrEqual => {
            if let Some(e) = greater_than_or_original(
                &original,
                &left,
                &right,
                timestamp_col,
                year_col,
                month_col,
                day_col,
            ) {
                return (e, true);
            }
        }
        _ => {}
    };
    let (left_part, left_added) =
        add_partitioned_timestamp_conditions(left, timestamp_col, year_col, month_col, day_col);
    let (right_part, right_added) =
        add_partitioned_timestamp_conditions(right, timestamp_col, year_col, month_col, day_col);

    (
        SimpleExpr::Binary(Box::new(left_part), op, Box::new(right_part)),
        left_added || right_added,
    )
}

fn greater_than_or_original(
    original: &SimpleExpr,
    left: &SimpleExpr,
    right: &SimpleExpr,
    timestamp_col: &str,
    year_col: &str,
    month_col: &str,
    day_col: &str,
) -> Option<SimpleExpr> {
    if let SimpleExpr::Column(left_column) = left {
        if let Some(colname) = find_colname(left_column) {
            if timestamp_col == &colname {
                if let SimpleExpr::Value(right_value) = right {
                    if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                        let right_year = right_dt.year();
                        let right_month = right_dt.month();
                        let right_day = right_dt.day();
                        let year_greater_than_year_expr = col_name_oper_const_num(
                            year_col.to_string(),
                            right_year,
                            BinOper::GreaterThan,
                        );
                        let year_equal_and_month_greater_expr = SimpleExpr::Binary(
                            Box::new(col_name_oper_const_num(
                                year_col.to_string(),
                                right_year,
                                BinOper::Equal,
                            )),
                            BinOper::And,
                            Box::new(col_name_oper_const_num(
                                month_col.to_string(),
                                right_month as i32,
                                BinOper::GreaterThan,
                            )),
                        );
                        let year_equal_and_month_equal_and_day_greater = SimpleExpr::Binary(
                            Box::new(year_equal_and_month_equal(
                                year_col.to_string(),
                                right_year,
                                month_col.to_string(),
                                right_month,
                            )),
                            BinOper::And,
                            Box::new(col_name_oper_const_num(
                                day_col.to_string(),
                                right_day as i32,
                                BinOper::GreaterThan,
                            )),
                        );
                        let year_equal_and_month_equal_and_day_equal_and_original =
                            SimpleExpr::Binary(
                                Box::new(year_equal_and_month_equal_and_day_equal(
                                    year_col.to_string(),
                                    right_year,
                                    month_col.to_string(),
                                    right_month,
                                    day_col.to_string(),
                                    right_day,
                                )),
                                BinOper::And,
                                Box::new(original.clone()),
                            );
                        return Some(iterated_binoper(
                            vec![
                                year_greater_than_year_expr,
                                year_equal_and_month_greater_expr,
                                year_equal_and_month_equal_and_day_greater,
                                year_equal_and_month_equal_and_day_equal_and_original,
                            ],
                            BinOper::Or,
                        ));
                    }
                }
            }
        } else if let SimpleExpr::Value(left_value) = &left {
            if let Value::ChronoDateTime(Some(left_dt)) = left_value {
                if let SimpleExpr::Column(right_column) = &right {
                    if let Some(colname) = find_colname(right_column) {
                        if timestamp_col == &colname {
                            let left_year = left_dt.year();
                            let left_month = left_dt.month();
                            let left_day = left_dt.day();
                            let year_greater_than_year_expr = const_num_oper_col_name(
                                left_year,
                                year_col.to_string(),
                                BinOper::GreaterThan,
                            );
                            let year_equal_and_month_greater_expr = SimpleExpr::Binary(
                                Box::new(const_num_oper_col_name(
                                    left_year,
                                    year_col.to_string(),
                                    BinOper::Equal,
                                )),
                                BinOper::And,
                                Box::new(const_num_oper_col_name(
                                    left_month as i32,
                                    month_col.to_string(),
                                    BinOper::GreaterThan,
                                )),
                            );
                            let year_equal_and_month_equal_and_day_greater = SimpleExpr::Binary(
                                Box::new(year_equal_and_month_equal(
                                    year_col.to_string(),
                                    left_year,
                                    month_col.to_string(),
                                    left_month,
                                )),
                                BinOper::And,
                                Box::new(col_name_oper_const_num(
                                    day_col.to_string(),
                                    left_day as i32,
                                    BinOper::GreaterThan,
                                )),
                            );
                            let year_equal_and_month_equal_and_day_equal_and_original =
                                SimpleExpr::Binary(
                                    Box::new(year_equal_and_month_equal_and_day_equal(
                                        year_col.to_string(),
                                        left_year,
                                        month_col.to_string(),
                                        left_month,
                                        day_col.to_string(),
                                        left_day,
                                    )),
                                    BinOper::And,
                                    Box::new(original.clone()),
                                );
                            return Some(iterated_binoper(
                                vec![
                                    year_greater_than_year_expr,
                                    year_equal_and_month_greater_expr,
                                    year_equal_and_month_equal_and_day_greater,
                                    year_equal_and_month_equal_and_day_equal_and_original,
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            }
        }
    }
    None
}

//Used for equal/non equal
fn oper_or_original(
    original: &SimpleExpr,
    left: &SimpleExpr,
    right: &SimpleExpr,
    oper: BinOper,
    timestamp_col: &str,
    year_col: &str,
    month_col: &str,
    day_col: &str,
) -> Option<SimpleExpr> {
    if let SimpleExpr::Column(left_column) = left {
        if let Some(colname) = find_colname(left_column) {
            if timestamp_col == &colname {
                if let SimpleExpr::Value(right_value) = right {
                    if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                        let right_year = right_dt.year();
                        let right_month = right_dt.month();
                        let right_day = right_dt.day();

                        let year_not_equal =
                            col_name_oper_const_num(year_col.to_string(), right_year, oper.clone());
                        let month_not_equal = col_name_oper_const_num(
                            month_col.to_string(),
                            right_month as i32,
                            oper.clone(),
                        );
                        let day_not_equal = col_name_oper_const_num(
                            day_col.to_string(),
                            right_day as i32,
                            oper.clone(),
                        );
                        return Some(iterated_binoper(
                            vec![
                                year_not_equal,
                                month_not_equal,
                                day_not_equal,
                                original.clone(),
                            ],
                            BinOper::Or,
                        ));
                    }
                }
            }
        }
    } else if let SimpleExpr::Value(left_value) = left {
        if let Value::ChronoDateTime(Some(left_dt)) = left_value {
            if let SimpleExpr::Column(right_column) = right {
                if let Some(colname) = find_colname(right_column) {
                    if timestamp_col == &colname {
                        let left_year = left_dt.year();
                        let left_month = left_dt.month();
                        let left_day = left_dt.day();

                        let year_not_equal =
                            const_num_oper_col_name(left_year, year_col.to_string(), oper.clone());
                        let month_not_equal = const_num_oper_col_name(
                            left_month as i32,
                            month_col.to_string(),
                            oper.clone(),
                        );
                        let day_not_equal = const_num_oper_col_name(
                            left_day as i32,
                            day_col.to_string(),
                            oper.clone(),
                        );
                        return Some(iterated_binoper(
                            vec![
                                year_not_equal,
                                month_not_equal,
                                day_not_equal,
                                original.clone(),
                            ],
                            BinOper::Or,
                        ));
                    }
                }
            }
        }
    }
    None
}

fn smaller_than_or_original(
    original: &SimpleExpr,
    left: &SimpleExpr,
    right: &SimpleExpr,
    timestamp_col: &str,
    year_col: &str,
    month_col: &str,
    day_col: &str,
) -> Option<SimpleExpr> {
    if let SimpleExpr::Column(left_column) = left {
        if let Some(colname) = find_colname(left_column) {
            if timestamp_col == &colname {
                if let SimpleExpr::Value(right_value) = right {
                    if let Value::ChronoDateTime(Some(right_dt)) = right_value {
                        let right_year = right_dt.year();
                        let right_month = right_dt.month();
                        let right_day = right_dt.day();
                        let year_smaller_than_year_expr = col_name_oper_const_num(
                            year_col.to_string(),
                            right_year,
                            BinOper::SmallerThan,
                        );
                        let year_equal_and_month_smaller_expr = SimpleExpr::Binary(
                            Box::new(col_name_oper_const_num(
                                year_col.to_string(),
                                right_year,
                                BinOper::Equal,
                            )),
                            BinOper::And,
                            Box::new(col_name_oper_const_num(
                                month_col.to_string(),
                                right_month as i32,
                                BinOper::SmallerThan,
                            )),
                        );
                        let year_equal_and_month_equal_and_day_smaller = SimpleExpr::Binary(
                            Box::new(year_equal_and_month_equal(
                                year_col.to_string(),
                                right_year,
                                month_col.to_string(),
                                right_month,
                            )),
                            BinOper::And,
                            Box::new(col_name_oper_const_num(
                                day_col.to_string(),
                                right_day as i32,
                                BinOper::SmallerThan,
                            )),
                        );
                        let year_equal_and_month_equal_and_day_equal_and_original =
                            SimpleExpr::Binary(
                                Box::new(year_equal_and_month_equal_and_day_equal(
                                    year_col.to_string(),
                                    right_year,
                                    month_col.to_string(),
                                    right_month,
                                    day_col.to_string(),
                                    right_day,
                                )),
                                BinOper::And,
                                Box::new(original.clone()),
                            );
                        return Some(iterated_binoper(
                            vec![
                                year_smaller_than_year_expr,
                                year_equal_and_month_smaller_expr,
                                year_equal_and_month_equal_and_day_smaller,
                                year_equal_and_month_equal_and_day_equal_and_original,
                            ],
                            BinOper::Or,
                        ));
                    }
                }
            }
        } else if let SimpleExpr::Value(left_value) = left {
            if let Value::ChronoDateTime(Some(left_dt)) = left_value {
                if let SimpleExpr::Column(right_column) = right {
                    if let Some(colname) = find_colname(right_column) {
                        if timestamp_col == &colname {
                            let left_year = left_dt.year();
                            let left_month = left_dt.month();
                            let left_day = left_dt.day();
                            let year_smaller_than_year_expr = const_num_oper_col_name(
                                left_year,
                                year_col.to_string(),
                                BinOper::SmallerThan,
                            );
                            let year_equal_and_month_smaller_expr = SimpleExpr::Binary(
                                Box::new(const_num_oper_col_name(
                                    left_year,
                                    year_col.to_string(),
                                    BinOper::Equal,
                                )),
                                BinOper::And,
                                Box::new(const_num_oper_col_name(
                                    left_month as i32,
                                    month_col.to_string(),
                                    BinOper::SmallerThan,
                                )),
                            );
                            let year_equal_and_month_equal_and_day_smaller = SimpleExpr::Binary(
                                Box::new(year_equal_and_month_equal(
                                    year_col.to_string(),
                                    left_year,
                                    month_col.to_string(),
                                    left_month,
                                )),
                                BinOper::And,
                                Box::new(col_name_oper_const_num(
                                    day_col.to_string(),
                                    left_day as i32,
                                    BinOper::SmallerThan,
                                )),
                            );
                            let year_equal_and_month_equal_and_day_equal_and_original =
                                SimpleExpr::Binary(
                                    Box::new(year_equal_and_month_equal_and_day_equal(
                                        year_col.to_string(),
                                        left_year,
                                        month_col.to_string(),
                                        left_month,
                                        day_col.to_string(),
                                        left_day,
                                    )),
                                    BinOper::And,
                                    Box::new(original.clone()),
                                );
                            return Some(iterated_binoper(
                                vec![
                                    year_smaller_than_year_expr,
                                    year_equal_and_month_smaller_expr,
                                    year_equal_and_month_equal_and_day_smaller,
                                    year_equal_and_month_equal_and_day_equal_and_original,
                                ],
                                BinOper::Or,
                            ));
                        }
                    }
                }
            }
        }
    }
    None
}

fn named_column_box_simple_expression(name: String) -> Box<SimpleExpr> {
    Box::new(SimpleExpr::Column(ColumnRef::Column(
        Name::Column(name).into_iden(),
    )))
}

fn year_equal_and_month_equal_and_day_equal(
    year_col: String,
    year: i32,
    month_col: String,
    month: u32,
    day_col: String,
    day: u32,
) -> SimpleExpr {
    SimpleExpr::Binary(
        Box::new(year_equal_and_month_equal(year_col, year, month_col, month)),
        BinOper::And,
        Box::new(col_name_oper_const_num(day_col, day as i32, BinOper::Equal)),
    )
}

fn year_equal_and_month_equal(
    year_col: String,
    year: i32,
    month_col: String,
    month: u32,
) -> SimpleExpr {
    SimpleExpr::Binary(
        Box::new(col_name_oper_const_num(year_col, year, BinOper::Equal)),
        BinOper::And,
        Box::new(col_name_oper_const_num(
            month_col,
            month as i32,
            BinOper::Equal,
        )),
    )
}

fn col_name_oper_const_num(col_name: String, num: i32, oper: BinOper) -> SimpleExpr {
    SimpleExpr::Binary(
        named_column_box_simple_expression(col_name),
        oper,
        Box::new(SimpleExpr::Value(Value::Int(Some(num)))),
    )
}

fn const_num_oper_col_name(num: i32, col_name: String, oper: BinOper) -> SimpleExpr {
    SimpleExpr::Binary(
        Box::new(SimpleExpr::Value(Value::Int(Some(num)))),
        oper,
        named_column_box_simple_expression(col_name),
    )
}

fn find_colname(cr: &ColumnRef) -> Option<String> {
    match cr {
        ColumnRef::Column(c) => Some(c.to_string()),
        ColumnRef::TableColumn(_, c) => Some(c.to_string()),
        ColumnRef::SchemaTableColumn(_, _, c) => Some(c.to_string()),
        _ => None,
    }
}

fn iterated_binoper(mut exprs: Vec<SimpleExpr>, oper: BinOper) -> SimpleExpr {
    let mut expr = exprs.remove(0);
    for e in exprs {
        expr = SimpleExpr::Binary(Box::new(expr), oper.clone(), Box::new(e))
    }
    expr
}
