from typing import Dict, Literal
from sqlalchemy.sql.base import ColumnCollection

from chrontext import Expression, VirtualizedQuery
from sqlalchemy import ColumnElement, Column, Table, MetaData, Select, select


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
    mapper = SPARQLMapper("BigQuery", "id", resource_table_map)
    sqlq = mapper.virtualized_query_to_sql(arg)
    print("\n")
    print(sqlq)


class SPARQLMapper:
    def __init__(self,
                 dialect: Literal["BigQuery"],
                 identity_column_name: str,
                 resource_table_map: Dict[str, Table]):
        self.dialect = dialect
        self.identity_column_name = identity_column_name
        self.resource_table_map = resource_table_map

    def virtualized_query_to_sql(self, query: VirtualizedQuery) -> Select:
        query_type = query.type_name()
        match query_type:
            case "FilteredVirtualizedQuery":
                sql_quer = self.virtualized_query_to_sql(query.query)
                filter_expr = self.expression_to_sql(query.filter, sql_quer.selected_columns)
                filtered = sql_quer.filter(filter_expr)
                return filtered

            case "BasicVirtualizedQuery":
                table = self.resource_table_map[query.resource]
                to_select = []
                for (k, v) in query.column_mapping.items():
                    to_select.append(table.columns[k].label(v))
                if query.grouping_column_name is not None:
                    if self.dialect == "BigQuery":
                        pass
                else:
                    to_select.append(
                        table.columns[self.identity_column_name].label(query.identifier_name)
                    )

                sql_q = select(*to_select)
                return sql_q

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
                return expression.literal.to_native()
            case _:
                print(type(expression))
                print(expression)
