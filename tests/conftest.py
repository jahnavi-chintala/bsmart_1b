import os
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from bsmart_gold.config import ConfigSet

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG = os.path.join(ROOT, "config")


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="bsmart_wh_")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("bsmart_gold_tests")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


def make_table(spark, name, rows, extra=()):
    cols = list(extra)
    for r in rows:
        cols += [c for c in r if c not in cols]
    schema = StructType([StructField(c, StringType()) for c in cols])
    data = [tuple(r.get(c) for c in cols) for r in rows]
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.createDataFrame(data, schema).write.format("parquet").saveAsTable(name)


@pytest.fixture(scope="session")
def silver(spark):
    """Two studies in shared silver tables: 32703 (open label) and BNT999-01 (blinded)."""
    for db in ("cdr", "odr"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    A, B = "BNT327-03", "BNT999-01"
    make_table(spark, "cdr.DM", [
        dict(PROJECT=A, SUBJECT="101-01-001", SITENUMBER="101-01", ETHNIC="Not Hispanic", SEX="Female",
             BRTHDAT_YYYY="1970", RACE_WHITE="1", RACE_ASIAN="1"),
        dict(PROJECT=A, SUBJECT="101-01-002", SITENUMBER="101-01", ETHNIC="Hispanic", SEX="Male",
             BRTHDAT_YYYY="1980", RACE_BLACK="1"),
        dict(PROJECT=A, SUBJECT="101-01-003", SITENUMBER="101-01", SEX="Male", BRTHDAT_YYYY="1990",
             RACE_OTHER="1", RACEOTH="Pacific"),
        dict(PROJECT=B, SUBJECT="201-01-001", SITENUMBER="201-01", SEX="Female", BRTHDAT_YYYY="1965",
             RACE_WHITE="1"),
    ], extra=["RACE_AMERIND", "RACE_NHAWII", "RACE_NR"])
    make_table(spark, "cdr.RAND", [
        dict(PROJECT=A, SUBJECT="101-01-001", RANDYN="Yes", DSSTDAT_RAW="05 Feb 2025", ARM="Arm A"),
        dict(PROJECT=A, SUBJECT="101-01-002", RANDYN="No", DSSTDAT_RAW=None, ARM=None),
        dict(PROJECT=B, SUBJECT="201-01-001", RANDYN="Yes", DSSTDAT_RAW="10 Mar 2025", ARM="Masked"),
    ])
    make_table(spark, "cdr.DD", [dict(PROJECT=A, SUBJECT="101-01-002", DDDAT_RAW="01 Jun 2025")])
    make_table(spark, "cdr.DSEOS", [dict(PROJECT=A, SUBJECT="101-01-002", DSSTDAT_RAW="20 May 2025")])
    make_table(spark, "cdr.EX", [
        dict(PROJECT=A, SUBJECT="101-01-001", EXTRT="BNT327/Atezolizumab Administration",
             EXSTDAT_RAW="12 Feb 2025", EXVOL="10"),
        dict(PROJECT=A, SUBJECT="101-01-001", EXTRT="BNT327/Atezolizumab Administration",
             EXSTDAT_RAW="06 Feb 2025", EXVOL=None),
        dict(PROJECT=A, SUBJECT="101-01-001", EXTRT="BNT327/Atezolizumab Administration",
             EXSTDAT_RAW="05 Mar 2025", EXVOL="10"),
        dict(PROJECT=A, SUBJECT="101-01-001", EXTRT="Chemo", EXSTDAT_RAW="01 Feb 2025", EXVOL="5"),
    ])
    make_table(spark, "cdr.SV", [
        dict(PROJECT=A, SUBJECT="101-01-001", INSTANCENAME="Screening", VISDAT="2025-01-20", VISSTAT="Yes"),
        dict(PROJECT=A, SUBJECT="101-01-002", INSTANCENAME="Screening", VISDAT="2025-01-22", VISSTAT="Yes"),
        dict(PROJECT=A, SUBJECT="101-01-001", INSTANCENAME="Day 1 of Cycle 4 (1)",
             VISDAT="2025-04-01", VISSTAT="Yes"),
        dict(PROJECT=A, SUBJECT="101-01-001", INSTANCENAME="Unscheduled (1)", VISDAT="2025-04-15",
             VISSTAT="Yes"),
        dict(PROJECT=A, SUBJECT="101-01-001", INSTANCENAME="Unscheduled (1)", VISDAT="2025-04-20",
             VISSTAT="No"),
    ])
    make_table(spark, "cdr.IC", [
        dict(PROJECT=A, SUBJECT="101-01-001", FORMOID="IC", DSSTDAT_RAW="10 Jan 2025", DSPROT_STD="v2.0"),
        dict(PROJECT=A, SUBJECT="101-01-001", FORMOID="ICR", DSSTDAT_RAW="02 Jan 2025", DSPROT_STD="2"),
        dict(PROJECT=A, SUBJECT="101-01-001", FORMOID="ICR", DSSTDAT_RAW="01 Apr 2025", DSPROT_STD="3.0"),
        dict(PROJECT=A, SUBJECT="101-01-002", FORMOID="ICO", DSSTDAT_RAW="01 Jan 2025", DSPROT_STD="2.0"),
    ])
    make_table(spark, "cdr.BE", [
        dict(PROJECT=A, SUBJECT="101-01-001", FORMOID="BEL", INSTANCENAME="Day 1 of Cycle 4 (1)",
             BEDAT="2025-04-01", BETIM="09:30", BEPERF="Yes", BETPT="Pre-dose"),
        dict(PROJECT=A, SUBJECT="101-01-001", FORMOID="BET", INSTANCENAME="Unscheduled (1)",
             BEDAT="2025-04-20", BESTAT="No", BEPERF="Yes", BECOND="Other", BECONDSP="Frozen"),
    ])
    make_table(spark, "cdr.PC", [
        dict(PROJECT=A, SUBJECT="101-01-001", INSTANCENAME="Day 1 of Cycle 4 (1)", PCDAT="2025-04-01",
             PCTIM="10:00", PCPERF="Yes", PCTPT="2h post-dose", DATAPAGENAME="PK Serum"),
    ], extra=["PCSTAT"])
    make_table(spark, "cdr.IRT", [dict(PROJECT=B, SUBJECT="201-01-001", TREATMENT_ARM="Drug 10mg")])
    make_table(spark, "odr.PARTICIPANT_STATUS", [
        dict(PROTOCOLID=A, SUBJECT="101-01-001", PARTICIPANT_STATUS="Screened",
             PARTICIPANT_STATUS_DATE="2025-01-20", __CURRENT_FLAG="Y"),
        dict(PROTOCOLID=A, SUBJECT="101-01-001", PARTICIPANT_STATUS="Enrolled",
             PARTICIPANT_STATUS_DATE="2025-02-05", __CURRENT_FLAG="Y"),
        dict(PROTOCOLID=A, SUBJECT="101-01-001", PARTICIPANT_STATUS="In Follow-up",
             PARTICIPANT_STATUS_DATE="2025-02-05", __CURRENT_FLAG="Y"),
        dict(PROTOCOLID=A, SUBJECT="101-01-003", PARTICIPANT_STATUS="Screen Failed",
             PARTICIPANT_STATUS_DATE="2025-01-25", __CURRENT_FLAG="Y"),
        dict(PROTOCOLID=A, SUBJECT="101-01-003", PARTICIPANT_STATUS="Completed",
             PARTICIPANT_STATUS_DATE="2025-03-25", __CURRENT_FLAG="N"),
    ])
    make_table(spark, "odr.STUDIES", [
        dict(PROTOCOLID=A, COMPOUNDNAME="BNT327", INDICATION="SCLC", PHASENAME="Phase II",
             STUDYSTATUS="Active", DESCRIPTION="A trial", SPONSORNAME="BioNTech",
             FIRSTPARTCIPANTFIRSTVISIT_ACTUALDATE="2024-11-01", PARTCIPANTSENROLLED_CURBASELINE="120",
             __CURRENT_FLAG="Y"),
        dict(PROTOCOLID=A, COMPOUNDNAME="old", __CURRENT_FLAG="N"),
        dict(PROTOCOLID=B, COMPOUNDNAME="BNT999", __CURRENT_FLAG="Y"),
    ], extra=["LASTPARTCIPANTLASTVISIT_ACTUALDATE", "TRAILEND_ACTUALDATE"])
    make_table(spark, "odr.TRIAL_SITE", [
        dict(STUDYNAME=A, STUDYSITENUMBER="101-01", STUDYSITESTATUS="Active", ISARCHIVED="0",
             VENDORNAMEOFSITE="Site One", COUNTRYNAME="Germany", __CURRENT_FLAG="Y",
             SITEFIRSTPTCPTFIRSTVISITACTUALDATE="2024-12-01"),
        dict(STUDYNAME=A, STUDYSITENUMBER="10101", STUDYSITESTATUS="Active", ISARCHIVED="0",
             __CURRENT_FLAG="Y"),
        dict(STUDYNAME=A, STUDYSITENUMBER="102-01", STUDYSITESTATUS="", ISARCHIVED="0",
             __CURRENT_FLAG="Y"),
        dict(STUDYNAME=A, STUDYSITENUMBER="103-01", STUDYSITESTATUS="Closed", ISARCHIVED="1",
             __CURRENT_FLAG="Y"),
    ], extra=["LINE1", "CITY", "STATEPROVINCE", "POSTALCODE", "SITELASTPTCPTLASTVISITACTUALDATE"])
    return spark


@pytest.fixture()
def cfg():
    """Shipped entity/mapping config, with test schemas and a second (blinded) study."""
    c = ConfigSet.load(*(os.path.join(CONFIG, f) for f in
                         ("study_config.csv", "entity_config.csv", "mapping_table.csv")))
    for row in c.studies:
        row.update(CDR_SCHEMA="cdr", ODR_SCHEMA="odr")
    c.studies.append(dict(STUDYID="99901", STUDY_NUMBER="BNT999-01", CDR_SCHEMA="cdr", ODR_SCHEMA="odr",
                          BLINDING_TYPE="BLINDED", FIRST_DOSE_TREATMENT="BNT999 Administration",
                          SCREENING_VISIT_NAME="Screening", IS_ACTIVE="Y"))
    # Blinded study: subject group comes from the IRT file instead of the EDC RAND form.
    base = next(e for e in c.entities if e["ENTITY_NAME"] == "SUBJECT_DETAILS_RANDOMIZED")
    c.entities.append(dict(base, STUDYID="99901", FROM_SQL=base["FROM_SQL"] +
                           " LEFT JOIN ${CDR_SCHEMA}.IRT IRT ON IRT.PROJECT = DM.PROJECT"
                           " AND IRT.SUBJECT = DM.SUBJECT"))
    for target in ("SUBJECT_GROUP", "TREATMENT_SCHEDULE"):
        base = next(m for m in c.mappings if m["ENTITY_NAME"] == "SUBJECT_DETAILS_RANDOMIZED"
                    and m["TARGET_COLUMN_NAME"] == target)
        c.mappings.append(dict(base, STUDYID="99901", MAPPING_TABLE_NAME="IRT",
                               MAPPING_COLUMN_NAME="TREATMENT_ARM", ETL_LOGIC=""))
    return c
