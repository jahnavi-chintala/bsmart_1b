"""Turn an EntityPlan into Spark SQL."""
from __future__ import annotations

import re

from .config import ColumnMapping, EntityPlan

PLACEHOLDER = re.compile(r"\$\{([A-Za-z0-9_]+)\}")


def substitute(text: str, params: dict, context: str) -> str:
    """Replace ``${NAME}`` with the study parameter NAME; fail on unknown names."""
    def repl(match):
        name = match.group(1).upper()
        if name not in params:
            raise KeyError(f"{context}: placeholder ${{{match.group(1)}}} has no value in study_config")
        return str(params[name]).replace("'", "''") if params[name] is not None else ""
    return PLACEHOLDER.sub(repl, text)


def column_expression(col: ColumnMapping) -> str:
    """ETL_LOGIC if given, else a straight copy of TABLE.COLUMN, else NULL."""
    if col.logic:
        expr = col.logic
    elif col.table and col.source_columns:
        if "," in col.source_columns or "," in col.table:
            raise ValueError(f"{col.target}: several source columns listed but no ETL_LOGIC")
        expr = f"{col.table}.{col.source_columns}"
    else:
        expr = "NULL"
    return f"CAST({expr} AS {col.data_type or 'STRING'}) AS `{col.target}`"


def build_select(plan: EntityPlan, params: dict) -> str:
    ctx = f"{plan.studyid}/{plan.entity}"
    select_list = ",\n  ".join(
        substitute(column_expression(c), params, f"{ctx}.{c.target}") for c in plan.columns
    )
    sql = f"SELECT\n  {select_list}\nFROM {substitute(plan.from_sql, params, ctx + ' FROM_SQL')}"
    if plan.where_sql:
        sql += f"\nWHERE {substitute(plan.where_sql, params, ctx + ' WHERE_SQL')}"
    return sql


def build_dedup(plan: EntityPlan, inner_sql: str) -> str:
    """Keep the first row per DEDUP_KEYS ordered by DEDUP_ORDER_BY."""
    if not plan.dedup_keys:
        return inner_sql
    keys = ", ".join(f"`{k}`" for k in plan.dedup_keys)
    order = plan.dedup_order_by or keys
    cols = ", ".join(f"`{c.target}`" for c in plan.columns)
    return (
        f"SELECT {cols} FROM (\n"
        f"  SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY {order}) AS _rn\n"
        f"  FROM (\n{inner_sql}\n  ) src\n) ranked\nWHERE _rn = 1"
    )


def build_entity_sql(plan: EntityPlan, params: dict) -> str:
    return build_dedup(plan, build_select(plan, params))
