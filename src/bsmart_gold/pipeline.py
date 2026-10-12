"""Silver -> gold pipeline driven by the mapping table.

One code path for every study: for each active study, each entity in the resolved config is
turned into a Spark SQL query over the silver tables, entities that feed the same gold table are
unioned, and the result replaces that study's slice of the gold table.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import ConfigSet
from .sql_builder import build_entity_sql

log = logging.getLogger("bsmart_gold")

META_COLUMNS = ["STUDYID", "_SOURCE_ENTITY", "_LOAD_TS"]


@dataclass
class StudyResult:
    studyid: str
    row_counts: dict
    error: str | None = None


def study_params(cfg: ConfigSet, studyid: str) -> dict:
    params = dict(cfg.study(studyid))
    params["STUDYID"] = studyid
    return params


def generate_sql(cfg: ConfigSet, studyid: str) -> dict[str, str]:
    """{entity: SQL} for one study. Needs no Spark session, used for dry runs and review."""
    params = study_params(cfg, studyid)
    return {p.entity: build_entity_sql(p, params) for p in cfg.plan(studyid)}


def build_study(spark: SparkSession, cfg: ConfigSet, studyid: str) -> dict[str, DataFrame]:
    """{gold_table: DataFrame} for one study."""
    params = study_params(cfg, studyid)
    by_table: dict[str, list[DataFrame]] = {}
    for plan in cfg.plan(studyid):
        df = spark.sql(build_entity_sql(plan, params))
        df = df.select(
            F.lit(studyid).alias("STUDYID"),
            F.lit(plan.entity).alias("_SOURCE_ENTITY"),
            F.current_timestamp().alias("_LOAD_TS"),
            *df.columns,
        )
        by_table.setdefault(plan.gold_table, []).append(df)
    return {
        table: reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
        for table, dfs in by_table.items()
    }


def _table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:  # older runtimes
        return any(t.name.lower() == name.split(".")[-1].lower()
                   for t in spark.catalog.listTables(name.rsplit(".", 1)[0]))


def write_gold(spark: SparkSession, df: DataFrame, table: str, studyid: str, fmt: str = "delta"):
    """Replace this study's rows in the gold table, leaving other studies untouched."""
    if not _table_exists(spark, table):
        df.write.format(fmt).partitionBy("STUDYID").saveAsTable(table)
    elif fmt == "delta":
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"STUDYID = '{studyid}'")
           .option("mergeSchema", "true")
           .saveAsTable(table))
    else:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        cols = spark.table(table).columns
        df.select(*cols).write.insertInto(table, overwrite=True)


def run(
    spark: SparkSession,
    cfg: ConfigSet,
    gold_schema: str,
    studies: list[str] | None = None,
    fmt: str = "delta",
    dry_run: bool = False,
) -> list[StudyResult]:
    """Build gold for the given studies (default: every active study in study_config)."""
    results = []
    for studyid in studies or cfg.active_studies():
        try:
            if dry_run:
                for entity, sql in generate_sql(cfg, studyid).items():
                    print(f"-- ===== {studyid} / {entity}\n{sql};\n")
                results.append(StudyResult(studyid, {}))
                continue
            counts = {}
            for table, df in build_study(spark, cfg, studyid).items():
                target = f"{gold_schema}.{table}"
                df = df.cache()
                counts[table] = df.count()
                if counts[table] == 0:
                    log.warning("%s: %s produced 0 rows - check STUDY_NUMBER and silver data",
                                studyid, table)
                write_gold(spark, df, target, studyid, fmt)
                df.unpersist()
                log.info("%s: wrote %d rows to %s", studyid, counts[table], target)
            results.append(StudyResult(studyid, counts))
        except Exception as exc:  # keep going so one bad study does not block the others
            log.exception("Study %s failed", studyid)
            results.append(StudyResult(studyid, {}, error=str(exc)))

    failed = [r for r in results if r.error]
    if failed:
        raise RuntimeError("Gold build failed for: " + "; ".join(
            f"{r.studyid}: {r.error.splitlines()[0]}" for r in failed))
    return results
