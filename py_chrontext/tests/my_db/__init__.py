import datetime
from typing import Dict, Literal, Any

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.base import ColumnCollection

from chrontext import Expression, VirtualizedQuery
from sqlalchemy import ColumnElement, Column, Table, MetaData, Select, select, literal, DateTime


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
    print(str(mapper.virtualized_query_to_sql(vq).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})))
    return str(mapper.virtualized_query_to_sql(vq).compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


class SPARQLMapper:
    def __init__(self,
                 dialect: Literal["bigquery", "postgres"],
                 resource_sql_map: Dict[str, Table]):
        self.dialect = dialect
        self.resource_sql_map = resource_sql_map

    def virtualized_query_to_sql(self, query: VirtualizedQuery) -> Select:
        query_type = query.type_name()
        match query_type:
            case "FilteredVirtualizedQuery":
                sql_quer = self.virtualized_query_to_sql(query.query)
                filter_expr = self.expression_to_sql(query.filter, sql_quer.selected_columns)
                filtered = sql_quer.filter(filter_expr)
                return filtered

            case "BasicVirtualizedQuery":
                table = self.resource_sql_map[query.resource]
                print(table)
                to_select = []
                for (k, v) in query.column_mapping.items():
                    to_select.append(table.columns[k].label(v))
                if query.grouping_column_name is not None:
                    if self.dialect == "bigquery":
                        pass
                else:
                    to_select.append(
                        table.columns["id"].label(query.identifier_name)
                    )

                sql_q = select(*to_select)
                return sql_q

            case "InnerJoinVirtualizedQuery":
                pass

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
