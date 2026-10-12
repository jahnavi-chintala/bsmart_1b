"""Load the three config tables and resolve them for one study.

Config tables (CSV files in ``config/`` or Delta tables with the same columns):

* study_config   - one row per study: schemas and study parameters used as ``${NAME}``
                   placeholders in SQL.
* entity_config  - one row per entity: which gold table it feeds, FROM/JOIN, WHERE, dedup.
* mapping_table  - one row per gold column: source table/column and ETL_LOGIC.

Rows with STUDYID = 'DEFAULT' apply to every study. A row with a real STUDYID overrides the
DEFAULT row with the same key (entity for entity_config, entity + target column for
mapping_table), so a new study only needs a study_config row plus whatever differs.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass, field

DEFAULT_STUDY = "DEFAULT"


def _is_active(row: dict) -> bool:
    return (row.get("IS_ACTIVE") or "Y").strip().upper() != "N"


def _clean(row: dict) -> dict:
    return {k.strip().upper(): ("" if v is None else str(v).strip()) for k, v in row.items()}


def load_rows(source: str, spark=None) -> list[dict]:
    """Read a config table from a CSV path or, with a Spark session, a table name."""
    if source.lower().endswith(".csv"):
        with open(source, newline="", encoding="utf-8") as f:
            return [_clean(r) for r in csv.DictReader(f)]
    if spark is None:
        raise ValueError(f"Reading table {source!r} needs a Spark session")
    return [_clean(r.asDict()) for r in spark.table(source).collect()]


@dataclass
class ColumnMapping:
    target: str
    table: str
    source_columns: str
    logic: str
    order: int
    data_type: str
    studyid: str


@dataclass
class EntityPlan:
    studyid: str
    entity: str
    gold_table: str
    from_sql: str
    where_sql: str
    dedup_keys: list[str]
    dedup_order_by: str
    columns: list[ColumnMapping] = field(default_factory=list)


@dataclass
class ConfigSet:
    studies: list[dict]
    entities: list[dict]
    mappings: list[dict]

    @classmethod
    def load(cls, study_source: str, entity_source: str, mapping_source: str, spark=None):
        return cls(load_rows(study_source, spark), load_rows(entity_source, spark),
                   load_rows(mapping_source, spark))

    def study(self, studyid: str) -> dict:
        for row in self.studies:
            if row["STUDYID"] == studyid:
                return row
        raise KeyError(f"Study {studyid!r} is not in study_config")

    def active_studies(self) -> list[str]:
        return [r["STUDYID"] for r in self.studies if _is_active(r)]

    def plan(self, studyid: str) -> list[EntityPlan]:
        """Effective entities and columns for one study (DEFAULT rows + study overrides)."""
        self.study(studyid)

        entities: dict[str, dict] = {}
        for scope in (DEFAULT_STUDY, studyid):
            for row in self.entities:
                if row["STUDYID"] == scope:
                    entities[row["ENTITY_NAME"]] = row

        columns: dict[tuple[str, str], dict] = {}
        for scope in (DEFAULT_STUDY, studyid):
            for row in self.mappings:
                if row["STUDYID"] == scope:
                    columns[(row["ENTITY_NAME"], row["TARGET_COLUMN_NAME"])] = row

        plans = []
        for name, ent in entities.items():
            if not _is_active(ent):
                continue
            cols = [
                ColumnMapping(
                    target=row["TARGET_COLUMN_NAME"],
                    table=row["MAPPING_TABLE_NAME"],
                    source_columns=row["MAPPING_COLUMN_NAME"],
                    logic=row["ETL_LOGIC"],
                    order=int(row["COLUMN_ORDER"] or 9999),
                    data_type=row.get("DATA_TYPE") or "STRING",
                    studyid=row["STUDYID"],
                )
                for (ent_name, _), row in columns.items()
                if ent_name == name and _is_active(row)
            ]
            if not cols:
                raise ValueError(f"Entity {name} has no active columns for study {studyid}")
            cols.sort(key=lambda c: (c.order, c.target))
            plans.append(EntityPlan(
                studyid=studyid,
                entity=name,
                gold_table=ent["GOLD_TABLE"],
                from_sql=ent["FROM_SQL"],
                where_sql=ent["WHERE_SQL"],
                dedup_keys=[k.strip() for k in ent.get("DEDUP_KEYS", "").split(",") if k.strip()],
                dedup_order_by=ent.get("DEDUP_ORDER_BY", ""),
                columns=cols,
            ))
        return plans
