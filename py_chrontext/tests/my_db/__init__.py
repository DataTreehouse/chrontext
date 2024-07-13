import datetime
from typing import Dict, Literal, Any, List, Union

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.base import ColumnCollection

from chrontext import Expression, VirtualizedQuery, AggregateExpression
from sqlalchemy import ColumnElement, Column, Table, MetaData, Select, select, literal, DateTime, values, func, cast, \
    BigInteger, CompoundSelect, and_, literal_column, case, TIMESTAMP

XSD = "http://www.w3.org/2001/XMLSchema#"
XSD_INTEGER = "<http://www.w3.org/2001/XMLSchema#integer>"
FLOOR_DATE_TIME_TO_SECONDS_INTERVAL = "<https://github.com/DataTreehouse/chrontext#FloorDateTimeToSecondsInterval>"


def query(arg):
    timestamp = Column("timestamp")
    value = Column("value")
    id = Column("id")
    metadata = MetaData()
    resource_table_map = {"my_resource": Table(
        "my_table",
        metadata,
        timestamp, value, id
    )}
    mapper = SPARQLMapper("BigQuery", resource_table_map)
    sqlq = mapper.virtualized_query_to_sql(arg)
    print("\n")
    print(sqlq)


def translate_sql(vq: VirtualizedQuery, dialect: Literal["bigquery", "postgres"],
                  resource_sql_map: Dict[str, Any]) -> str:
    mapper = SPARQLMapper(dialect, resource_sql_map)
    q = mapper.virtualized_query_to_sql(vq)
    compiled = q.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    print("\n")
    print(compiled)
    return str(compiled)


class SPARQLMapper:
    def __init__(self,
                 dialect: Literal["bigquery", "postgres"],
                 resource_sql_map: Dict[str, Union[Table, CompoundSelect]]):
        self.dialect = dialect
        self.resource_sql_map = resource_sql_map

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
                table = table.subquery("inner")

                to_select = []
                to_select.append(
                    table.columns["id"].label(query.identifier_name)
                )
                for (k, v) in query.column_mapping.items():
                    to_select.append(table.columns[k].label(v))
                print(to_select)
                if query.grouping_column_name is not None:
                    if self.dialect == "bigquery":
                        pass
                    elif self.dialect == "postgres":
                        values_sub = values(
                            Column("id"), Column(query.grouping_column_name),
                            name="grouping"
                        ).data(
                            [(id, group) for (id, group) in query.id_to_grouping_mapping.items()]
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
                pass

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
            case "GROUP_CONCAT":
                if aggregate_expression.separator is not None:
                    return func.aggregate_strings(sql_expression,
                                                  separator=literal_column(f"'{aggregate_expression.separator}'"))
                else:
                    return func.aggregate_strings(sql_expression, separator=literal_column("''"))
            case _:
                print(aggregate_expression.name)

    def expression_to_sql(
            self,
            expression: Expression,
            columns: ColumnCollection[str, ColumnElement]
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
                return literal(native)
            case "FunctionCall":
                sql_args = []
                for a in expression.arguments:
                    sql_args.append(self.expression_to_sql(a, columns))
                return self.function_call_to_sql(expression.function, sql_args, columns)
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
                print(type(expression))
                print(expression)

    def function_call_to_sql(self,
                             function: str,
                             sql_args: List[Column | ColumnElement | int | float | bool | str],
                             columns: ColumnCollection[str, ColumnElement]) -> ColumnElement:
        match function:
            case "SECONDS":
                if self.dialect == "postgres":
                    return func.extract("SECOND", sql_args[0])
            case "MINUTES":
                if self.dialect == "postgres":
                    return func.extract("MINUTE", sql_args[0])
            case "HOURS":
                if self.dialect == "postgres":
                    return func.extract("HOUR", sql_args[0])
            case "DAY":
                if self.dialect == "postgres":
                    return func.extract("DAY", sql_args[0])
            case "MONTH":
                if self.dialect == "postgres":
                    return func.extract("MONTH", sql_args[0])
            case "YEAR":
                if self.dialect == "postgres":
                    return func.extract("YEAR", sql_args[0])
            case "FLOOR":
                return func.floor(sql_args[0])
            case "CEILING":
                return func.ceiling(sql_args[0])
            case IRI:
                if IRI == XSD_INTEGER:
                    return func.cast(sql_args[0], BigInteger)
                elif IRI == FLOOR_DATE_TIME_TO_SECONDS_INTERVAL:
                    return func.to_timestamp(
                            func.extract("EPOCH", sql_args[0]) - func.mod(
                                func.extract("EPOCH", sql_args[0]),
                                sql_args[1])
                            )
                print(IRI)
                print("PANIKK!!")
