pub const PYTHON_CODE: &str = r#"
from datetime import datetime
from typing import Dict, Literal, Any, List, Union

import sqlalchemy.types as types
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.base import ColumnCollection
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy_bigquery.base import BigQueryDialect
from databricks.sqlalchemy import DatabricksDialect

from chrontext.vq import Expression, VirtualizedQuery, AggregateExpression, XSDDuration
from sqlalchemy import ColumnElement, Column, Table, MetaData, Select, select, literal, DateTime, values, cast, \
    BigInteger, CompoundSelect, and_, literal_column, case, func, TIMESTAMP, text

XSD = "http://www.w3.org/2001/XMLSchema#"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
XSD_DURATION = "http://www.w3.org/2001/XMLSchema#duration"
FLOOR_DATE_TIME_TO_SECONDS_INTERVAL = "https://github.com/DataTreehouse/chrontext#FloorDateTimeToSecondsInterval"
DATE_BIN = "https://github.com/DataTreehouse/chrontext#dateBin"

import warnings

with warnings.catch_warnings():
    warnings.filterwarnings("ignore")
    class unnest(GenericFunction):
        name = "UNNEST"
        package = "bq"
        inherit_cache = True

def translate_sql(vq: VirtualizedQuery, dialect: Literal["bigquery", "postgres", "databricks"],
                  resource_sql_map: Dict[str, Any]) -> str:
    mapper = SPARQLMapper(dialect, resource_sql_map)
    q = mapper.virtualized_query_to_sql(vq)
    match dialect:
        case "bigquery":
            use_dialect = BigQueryDialect()
        case "postgres":
            use_dialect = postgresql.dialect()
        case "databricks":
            use_dialect = DatabricksDialect()
    compiled = q.compile(dialect=use_dialect, compile_kwargs={"literal_binds": True})
    return str(compiled)


class SPARQLMapper:
    def __init__(self,
                 dialect: Literal["bigquery", "postgres"],
                 resource_sql_map: Dict[str, Union[Table, CompoundSelect]]):
        self.dialect = dialect
        self.resource_sql_map = resource_sql_map
        self.counter = 0

    def virtualized_query_to_sql(self, query: VirtualizedQuery) -> Select:
        query_type = query.type_name()

        match query_type:
            case "Filtered":
                sql_quer = self.virtualized_query_to_sql(query.query)
                filter_expr = self.expression_to_sql(query.filter, sql_quer.selected_columns)
                filtered = sql_quer.filter(filter_expr)
                return filtered

            case "Basic":
                table = self.resource_sql_map[query.resource]
                if isinstance(table, Table):
                    table = select(*[t for t in table.columns])
                table = table.subquery(self.inner_name())

                to_select = []
                to_select.append(
                    table.columns["id"].label(query.identifier_name)
                )
                for (k, v) in query.column_mapping.items():
                    to_select.append(table.columns[k].label(v))
                if query.grouping_column_name is not None:
                    if self.dialect == "bigquery":
                        structs = []
                        for (id, group) in query.id_grouping_tuples:
                            structs.append(f"STRUCT('{id}' as id, {group} as {query.grouping_column_name})")
                        values_sub = func.bq.unnest(literal_column(f"[{', '.join(structs)}]")).table_valued(
                            Column("id"),
                            Column(query.grouping_column_name)
                        )
                        table = values_sub.join(
                            table,
                            onclause=and_(
                                values_sub.columns["id"] == table.columns["id"],
                                table.columns["id"].in_(query.ids)
                            )
                        )
                        to_select.append(
                            cast(
                                values_sub.columns[query.grouping_column_name], BigInteger
                            ).label(
                                query.grouping_column_name
                            )
                        )

                        sql_q = select(
                            *to_select
                        ).select_from(
                            table
                        )
                    if self.dialect == "postgres" or self.dialect == "databricks":
                        values_sub = values(
                            Column("id"), Column(query.grouping_column_name),
                            name=self.inner_name()
                        ).data(
                            query.id_grouping_tuples
                        )
                        table = values_sub.join(
                            table,
                            onclause=and_(
                                values_sub.columns["id"] == table.columns["id"],
                                table.columns["id"].in_(query.ids)
                            )
                        )
                        to_select.append(
                            cast(
                                values_sub.columns[query.grouping_column_name], BigInteger
                            ).label(
                                query.grouping_column_name
                            )
                        )
                        sql_q = select(
                            *to_select
                        ).select_from(
                            table
                        )
                else:
                    sql_q = select(
                        *to_select
                    ).where(
                        table.columns["id"].in_(query.ids)
                    )
                return sql_q

            case "Grouped":
                sql_quer = self.virtualized_query_to_sql(query.query)
                by = [sql_quer.columns[c.name].label(c.name) for c in query.by]
                selection = by.copy()
                for (v, agg) in query.aggregations:
                    selection.append(
                        self.aggregation_expression_to_sql(agg, sql_quer.columns).label(v.name)
                    )
                sql_quer = select(
                    sql_quer.subquery("inner")
                ).with_only_columns(
                    *selection
                ).group_by(
                    *by
                )
                return sql_quer

            case "ExpressionAs":
                sql_quer = self.virtualized_query_to_sql(query.query)
                sql_expression = self.expression_to_sql(query.expression, sql_quer.selected_columns)
                sql_quer = sql_quer.add_columns(sql_expression.label(query.variable.name))
                return sql_quer
            case "InnerJoin":
                sql_queries = []
                for q in query.queries:
                    sql_quer = self.virtualized_query_to_sql(q)
                    sql_queries.append(sql_quer)
                cols_keys = set()
                cols = []
                out_sql_quer = sql_queries.pop().subquery(self.inner_name())
                for c in out_sql_quer.columns.keys():
                    cols_keys.add(c)
                    cols.append(out_sql_quer.columns[c].label(c))
                for sql_quer in sql_queries:
                    on = None
                    sql_quer = sql_quer.subquery(self.inner_name())
                    for c in sql_quer.columns.keys():
                        if c not in cols_keys:
                            cols_keys.add(c)
                            cols.append(sql_quer.columns[c].label(c))
                        if c in out_sql_quer.columns:
                            new_on = out_sql_quer.columns[c] == sql_quer.columns[c]
                            if on is not None:
                                on = on & new_on
                            else:
                                on = new_on

                    out_sql_quer = out_sql_quer.join(sql_quer, onclause=on)

                out_sql_quer = select(*cols).select_from(out_sql_quer)
                return out_sql_quer
            case "Ordered":
                sql_quer = self.virtualized_query_to_sql(query.query)
                for o in query.ordering:
                    sql_expr = self.expression_to_sql(o.expression, sql_quer.selected_columns)
                    if o.ascending:
                        sql_expr_order = sql_expr.asc()
                    else:
                        sql_expr_order = sql_expr.desc()
                    sql_quer = sql_quer.order_by(sql_expr_order)
                return sql_quer

            case "Sliced":
                sql_quer = self.virtualized_query_to_sql(query.query)
                if query.offset > 0:
                    sql_quer = sql_quer.offset(query.offset)
                if query.limit is not None:
                    sql_quer = sql_quer.limit(query.limit)
                return sql_quer


    def aggregation_expression_to_sql(
            self,
            aggregate_expression: AggregateExpression,
            columns: ColumnCollection[str, ColumnElement],
    ) -> ColumnElement:
        sql_expression = self.expression_to_sql(aggregate_expression.expression, columns)
        match aggregate_expression.name:
            case "MIN":
                return func.min(sql_expression)
            case "MAX":
                return func.max(sql_expression)
            case "AVG":
                return func.avg(sql_expression)
            case "SUM":
                return func.sum(sql_expression)
            case "COUNT":
                return func.count(sql_expression)
            case "GROUP_CONCAT":
                if aggregate_expression.separator is not None:
                    return func.aggregate_strings(sql_expression,
                                                  separator=literal_column(f"'{aggregate_expression.separator}'"))
                else:
                    return func.aggregate_strings(sql_expression, separator=literal_column("''"))
            case _:
                print("Unknown aggregate expression")
                print(aggregate_expression.name)
                assert False

    def expression_to_sql(
            self,
            expression: Expression,
            columns: ColumnCollection[str, ColumnElement],
    ) -> Column | ColumnElement | int | float | bool | str:
        expression_type = expression.expression_type()
        match expression_type:
            case "Variable":
                return columns[expression.variable.name]
            case "Bound":
                return columns[expression.variable.name] != None
            case "Greater":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql > right_sql
            case "Less":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql < right_sql
            case "GreaterOrEqual":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql >= right_sql
            case "LessOrEqual":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql <= right_sql
            case "Equal":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql == right_sql
            case "And":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql & right_sql
            case "Or":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql | right_sql
            case "If":
                left_sql = self.expression_to_sql(expression.left, columns)
                middle_sql = self.expression_to_sql(expression.middle, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return case(left_sql, middle_sql, else_=right_sql)
            case "Not":
                expression_sql = self.expression_to_sql(expression.expression, columns)
                return ~expression_sql
            case "Multiply":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql * right_sql
            case "Divide":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql / right_sql
            case "Add":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql + right_sql
            case "Subtract":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql - right_sql
            case "Literal":
                native = expression.literal.to_native()
                if type(native) == datetime:
                    if native.tzinfo is not None:
                        return literal(native, TIMESTAMP)
                elif expression.literal.datatype.iri == XSD_DURATION:
                    if self.dialect == "bigquery":
                        return bigquery_duration_literal(native)
                return literal(native)
            case "FunctionCall":
                sql_args = []
                for a in expression.arguments:
                    sql_args.append(self.expression_to_sql(a, columns))
                return self.function_call_to_sql(expression.function, sql_args)
            case "In":
                sql_collection = []
                for e in expression.expressions:
                    sql_collection.append(self.expression_to_sql(e, columns))
                sql_expression = self.expression_to_sql(expression.expression, columns)
                return sql_expression.in_(sql_collection)
            case "Coalesce":
                sql_collection = []
                for e in expression.expressions:
                    sql_collection.append(self.expression_to_sql(e, columns))
                return func.coalesce(sql_collection)
            case _:
                print("Unknown expression")
                print(type(expression))
                print(expression)
                assert False

    def function_call_to_sql(self,
                             function: str,
                             sql_args: List[Column | ColumnElement | int | float | bool | str]) -> ColumnElement:
        match function:
            case "SECONDS":
                return func.extract("SECOND", sql_args[0])
            case "MINUTES":
                return func.extract("MINUTE", sql_args[0])
            case "HOURS":
                return func.extract("HOUR", sql_args[0])
            case "DAY":
                return func.extract("DAY", sql_args[0])
            case "MONTH":
                return func.extract("MONTH", sql_args[0])
            case "YEAR":
                return func.extract("YEAR", sql_args[0])
            case "FLOOR":
                return func.floor(sql_args[0])
            case "CEILING":
                return func.ceiling(sql_args[0])
            case IRI:
                if IRI == XSD_INTEGER:
                    return func.cast(sql_args[0], BigInteger)
                elif IRI == FLOOR_DATE_TIME_TO_SECONDS_INTERVAL:
                    if self.dialect == "postgres":
                        return func.to_timestamp(
                            func.extract("EPOCH", sql_args[0]) - func.mod(
                                func.extract("EPOCH", sql_args[0]),
                                sql_args[1])
                        )
                    elif self.dialect == "databricks":
                        return func.TIMESTAMP_SECONDS(
                            func.UNIX_TIMESTAMP(sql_args[0]) - func.mod(
                                func.UNIX_TIMESTAMP(sql_args[0]),
                                sql_args[1])
                        )
                    elif self.dialect == "bigquery":
                        return func.TIMESTAMP_SECONDS(
                            func.UNIX_SECONDS(sql_args[0]) - func.mod(
                                func.UNIX_SECONDS(sql_args[0]),
                                sql_args[1])
                        )
                elif IRI == DATE_BIN:
                    if self.dialect == "bigquery":
                        # https://cloud.google.com/bigquery/docs/reference/standard-sql/time-series-functions#timestamp_bucket
                        # Duration is second arg here
                        print(sql_args[0])
                        return func.TIMESTAMP_BUCKET(sql_args[1], sql_args[0], sql_args[2])

        print("Unknown function")
        print(function)
        assert False

    def inner_name(self) -> str:
        name = f"inner_{self.counter}"
        self.counter += 1
        return name


def bigquery_duration_literal(native:XSDDuration):
    f = None
    s = ""

    last = None
    if native.years > 0:
        f = "YEAR"
        last = "YEAR"
        s += str(native.years) + "-"

    if native.months > 0 or last is not None:
        if f is None:
            f = "MONTH"
        last = "MONTH"
        s += str(native.months)

    if native.days > 0 or last is not None:
        if f is None:
            f = "DAY"
        last = "DAY"
        if len(s) > 0:
            s += " "
        s += str(native.days)

    if native.hours > 0 or last is not None:
        if f is None:
            f = "HOUR"
        last = "HOUR"
        if len(s) > 0:
            s += " "
        s += str(native.hours)

    if native.minutes > 0 or last is not None:
        if f is None:
            f = "MINUTE"
        last = "MINUTE"
        if len(s) > 0:
            s += ":"
        s += str(native.minutes)

    whole,decimal = native.seconds

    if whole > 0 or last is not None:
        if f is None:
            f = "SECOND"
        last = "SECOND"
        if len(s) > 0:
            s += ":"
        s += str(whole)

    return text(f"INTERVAL '{s}' {f} TO {last}")
"#;
