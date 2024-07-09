from typing import Dict, Any

import sqlalchemy.sql.expression

from chrontext import Expression
from sqlalchemy import ColumnElement, Column, literal


def query(arg):
    map = {"t": Column("t"),
           "v": Column("v")}
    mapper = SPARQLMapper(map)

    ex = mapper.expression_to_sql(arg.filtered.filter)
    print(ex)


class SPARQLMapper:
    def __init__(self, variable_map: Dict[str, Column]):
        self.variable_map = variable_map

    def expression_to_sql(self, expression: Expression) -> Column | ColumnElement | int | float | bool | str:
        expression_type = expression.expression_type()

        match expression_type:
            case "Variable":
                return self.variable_map[expression.variable.name]
            case "Greater":
                left_sql = self.expression_to_sql(expression.left)
                right_sql = self.expression_to_sql(expression.right)
                return left_sql > right_sql
            case "Less":
                left_sql = self.expression_to_sql(expression.left)
                right_sql = self.expression_to_sql(expression.right)
                return left_sql < right_sql
            case "And":
                left_sql = self.expression_to_sql(expression.left)
                right_sql = self.expression_to_sql(expression.right)
                return left_sql & right_sql
            case "Literal":
                return expression.literal.to_native()
            case _:
                print(type(expression))
                print(expression)
