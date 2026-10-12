"""Command line entry point.

    python -m bsmart_gold --gold-schema gold.bsmart                 # all active studies
    python -m bsmart_gold --gold-schema gold.bsmart --study 32703   # one study
    python -m bsmart_gold --study 32703 --dry-run                   # print the SQL only
"""
from __future__ import annotations

import argparse
import logging
import os

from .config import ConfigSet

CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "config")


def main(argv=None):
    p = argparse.ArgumentParser(description="Build B-SMART gold tables from silver")
    p.add_argument("--study", action="append", help="STUDYID to run (repeatable); default all active")
    p.add_argument("--gold-schema", default="gold.bsmart")
    p.add_argument("--format", default="delta", help="Table format for gold (delta on Databricks)")
    p.add_argument("--study-config", default=os.path.join(CONFIG_DIR, "study_config.csv"))
    p.add_argument("--entity-config", default=os.path.join(CONFIG_DIR, "entity_config.csv"))
    p.add_argument("--mapping-table", default=os.path.join(CONFIG_DIR, "mapping_table.csv"))
    p.add_argument("--dry-run", action="store_true", help="Print generated SQL, do not run")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    spark = None
    sources = (args.study_config, args.entity_config, args.mapping_table)
    if not args.dry_run or not all(s.lower().endswith(".csv") for s in sources):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("bsmart_gold").getOrCreate()

    cfg = ConfigSet.load(*sources, spark=spark)

    from .pipeline import generate_sql, run
    if args.dry_run and spark is None:
        for studyid in args.study or cfg.active_studies():
            for entity, sql in generate_sql(cfg, studyid).items():
                print(f"-- ===== {studyid} / {entity}\n{sql};\n")
        return
    for r in run(spark, cfg, args.gold_schema, args.study, args.format, args.dry_run):
        print(r.studyid, r.row_counts)


if __name__ == "__main__":
    main()
