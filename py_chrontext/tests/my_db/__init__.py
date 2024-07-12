import datetime
from typing import Dict, Literal, Any, List

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.base import ColumnCollection

from chrontext import Expression, VirtualizedQuery, AggregateExpression
from sqlalchemy import ColumnElement, Column, Table, MetaData, Select, select, literal, DateTime, values, func


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


def translate_sql(vq:VirtualizedQuery, dialect:Literal["bigquery", "postgres"], resource_sql_map:Dict[str, Any]) -> str:
    mapper = SPARQLMapper(dialect, resource_sql_map)
    q = mapper.virtualized_query_to_sql(vq)
    compiled = q.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    print(compiled)
    return str(compiled)


class SPARQLMapper:
    def __init__(self,
                 dialect: Literal["bigquery", "postgres"],
                 resource_sql_map: Dict[str, Table]):
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
                to_select = []
                for (k, v) in query.column_mapping.items():
                    to_select.append(table.columns[k].label(v))
                if query.grouping_column_name is not None:
                    if self.dialect == "bigquery":
                        pass
                    elif self.dialect == "postgres":
                        table = values(
                            Column("id"),
                            Column("grouping")
                        ).data(
                            [v for v in query.id_to_grouping_mapping.items()]
                        ).select().join(table.subquery(), onclause=Column("id"))
                        to_select.append(table.columns["grouping"].label(query.grouping_column_name))
                else:
                    to_select.append(
                        table.columns["id"].label(query.identifier_name)
                    )
                sql_q = table.select().add_columns(*to_select)
                return sql_q

            case "Grouped":
                sql_quer = self.virtualized_query_to_sql(query.query)
                by = [sql_quer.selected_columns[c.name] for c in query.by]
                selection = by.copy()
                for (v,agg) in query.aggregations:
                    selection.append(
                        self.aggregation_expression_to_sql(agg, sql_quer.selected_columns).label(v.name)
                    )

                sql_quer = select(*selection).group_by(*by)
                return sql_quer

            case "InnerJoin":
                pass


    def aggregation_expression_to_sql(
            self,
            aggregate_expression:AggregateExpression,
        columns: ColumnCollection[str, ColumnElement],
    ) -> ColumnElement:
        sql_expression = self.expression_to_sql(aggregate_expression.expression, columns)
        match aggregate_expression.name:
            case "SUM":
                return func.sum(sql_expression)
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
            case "Greater":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql > right_sql
            case "Less":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql < right_sql
            case "And":
                left_sql = self.expression_to_sql(expression.left, columns)
                right_sql = self.expression_to_sql(expression.right, columns)
                return left_sql & right_sql
            case "Literal":
                native = expression.literal.to_native()
                #if type(native) == datetime.datetime:
                #    return literal(native)
                return literal(native)
            case _:
                print(type(expression))
                print(expression)
