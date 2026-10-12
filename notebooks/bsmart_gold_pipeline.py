# Databricks notebook source
# MAGIC %md
# MAGIC # B-SMART silver -> gold
# MAGIC One notebook for every study. What it builds comes from the mapping table, not from code:
# MAGIC
# MAGIC * `study_config` - one row per study (schemas, study number, blinding, ...)
# MAGIC * `entity_config` - per entity: gold table, FROM/JOIN, WHERE, dedup
# MAGIC * `mapping_table` - per gold column: source table/column and ETL_LOGIC
# MAGIC
# MAGIC `STUDYID = 'DEFAULT'` rows apply to all studies; rows with a real STUDYID override them.
# MAGIC To onboard a study, add a `study_config` row (plus override rows only where it differs).

# COMMAND ----------

dbutils.widgets.text("studies", "", "STUDYIDs (comma separated, empty = all active)")
dbutils.widgets.text("gold_schema", "gold.bsmart", "Gold schema")
dbutils.widgets.text("config_source", "", "Config schema (empty = CSVs in repo)")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"], "Dry run (print SQL only)")

# COMMAND ----------

import logging
import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(repo_root, "src"))
logging.basicConfig(level=logging.INFO)

from bsmart_gold.config import ConfigSet
from bsmart_gold.pipeline import run

config_source = dbutils.widgets.get("config_source").strip()
if config_source:
    sources = [f"{config_source}.{t}" for t in ("study_config", "entity_config", "mapping_table")]
else:
    sources = [os.path.join(repo_root, "config", f)
               for f in ("study_config.csv", "entity_config.csv", "mapping_table.csv")]
cfg = ConfigSet.load(*sources, spark=spark)

studies = [s.strip() for s in dbutils.widgets.get("studies").split(",") if s.strip()] or None

# COMMAND ----------

results = run(
    spark,
    cfg,
    gold_schema=dbutils.widgets.get("gold_schema"),
    studies=studies,
    dry_run=dbutils.widgets.get("dry_run") == "true",
)
display(spark.createDataFrame(
    [(r.studyid, t, n) for r in results for t, n in r.row_counts.items()] or [("", "", 0)],
    "STUDYID string, GOLD_TABLE string, ROWS long",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: publish the repo CSVs as Delta config tables
# MAGIC Run once (or after editing the CSVs) if you prefer to keep the mapping in tables.

# COMMAND ----------

# target = "config.bsmart"
# for name in ("study_config", "entity_config", "mapping_table"):
#     (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
#          .csv(f"file:{repo_root}/config/{name}.csv")
#          .write.mode("overwrite").saveAsTable(f"{target}.{name}"))
