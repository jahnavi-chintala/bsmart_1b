import pytest

from bsmart_gold.pipeline import generate_sql, run
from bsmart_gold.sql_builder import substitute

GOLD = "gold_test"


def rows(spark, table, studyid, order_by=("SUBJECT",)):
    df = spark.table(f"{GOLD}.{table}").where(f"STUDYID = '{studyid}'")
    return [r.asDict() for r in df.orderBy(*order_by).collect()]


@pytest.fixture(scope="module")
def gold(silver, request):
    silver.sql(f"DROP DATABASE IF EXISTS {GOLD} CASCADE")
    silver.sql(f"CREATE DATABASE {GOLD}")
    return silver


@pytest.fixture()
def built(gold, cfg):
    run(gold, cfg, GOLD, fmt="parquet")
    return gold


# ------------------------------------------------------------------ config resolution

def test_default_rows_apply_to_every_study(cfg):
    for studyid in ("32703", "99901"):
        entities = {p.entity for p in cfg.plan(studyid)}
        assert entities == {"SUBJECT_DETAILS_SCREENING", "SUBJECT_DETAILS_RANDOMIZED", "SUBJECT_CONSENT",
                            "SUBJECT_VISIT_BE", "SUBJECT_VISIT_PC", "STUDY_DETAILS", "STUDY_SITES"}


def test_study_rows_override_defaults(cfg):
    def group_col(studyid):
        plan = next(p for p in cfg.plan(studyid) if p.entity == "SUBJECT_DETAILS_RANDOMIZED")
        return next(c for c in plan.columns if c.target == "SUBJECT_GROUP")

    assert (group_col("32703").table, group_col("32703").studyid) == ("RAND", "DEFAULT")
    assert (group_col("99901").table, group_col("99901").studyid) == ("IRT", "99901")


def test_generated_sql_has_no_unresolved_placeholders(cfg):
    for studyid in cfg.active_studies():
        for sql in generate_sql(cfg, studyid).values():
            assert "${" not in sql


def test_unknown_placeholder_fails_loudly():
    with pytest.raises(KeyError, match="MISSING"):
        substitute("SELECT '${MISSING}'", {"STUDYID": "1"}, "ctx")


# ------------------------------------------------------------------ gold output

def test_subject_details(built):
    out = rows(built, "SUBJECT_DETAILS", "32703", ("SUBJECT", "_SOURCE_ENTITY"))
    by_key = {(r["SUBJECT"], r["_SOURCE_ENTITY"].rsplit("_", 1)[1]): r for r in out}
    # one screening record per subject, a second record only for randomized subjects
    assert sorted(by_key) == [("101-01-001", "RANDOMIZED"), ("101-01-001", "SCREENING"),
                              ("101-01-002", "SCREENING"), ("101-01-003", "SCREENING")]

    scr, rnd = by_key[("101-01-001", "SCREENING")], by_key[("101-01-001", "RANDOMIZED")]
    assert scr["RACE"] == "Mixed : White,Asian"
    assert scr["MORTALITY_STATUS"] == "Alive"
    assert scr["PARTICIPANT_FIRST_DOSE_DATE"] == "2025-02-12"   # EXVOL null and other EXTRT ignored
    assert scr["ENROLLMENT_STATUS"] == "In Follow-up"           # same date: higher priority wins
    assert scr["ENROLLMENT_DATE"] == "2025-02-05"
    assert (scr["RANDOMIZATION_DATE"], scr["SUBJECT_GROUP"], scr["TREATMENT_SCHEDULE"]) == \
        ("2025-01-20", "Screening", "Screening")
    assert (rnd["RANDOMIZATION_DATE"], rnd["SUBJECT_GROUP"], rnd["TREATMENT_SCHEDULE"]) == \
        ("2025-02-05", "Arm A", "Arm A")
    assert scr["STUDY_NUMBER"] == "BNT327-03" and scr["SITE_NUMBER"] == "101-01"
    assert scr["PROTOCOL_VERSION"] is None

    s2 = by_key[("101-01-002", "SCREENING")]
    assert (s2["RACE"], s2["MORTALITY_STATUS"]) == ("Black or African American", "Dead")
    assert (s2["DISCONTINUATION_DATE"], s2["ENROLLMENT_STATUS"]) == ("2025-05-20", "Discontinued")

    s3 = by_key[("101-01-003", "SCREENING")]
    assert s3["RACE"] == "Mixed : Race Other,Race Other Specify : Pacific"
    assert s3["ENROLLMENT_STATUS"] == "Screen Failed"            # non-current ODR rows ignored
    assert s3["ENROLLMENT_DATE"] is None


def test_blinded_study_uses_overrides(built):
    out = rows(built, "SUBJECT_DETAILS", "99901", ("_SOURCE_ENTITY",))
    assert [r["_SOURCE_ENTITY"] for r in out] == ["SUBJECT_DETAILS_RANDOMIZED", "SUBJECT_DETAILS_SCREENING"]
    assert all(r["MORTALITY_STATUS"] is None for r in out)
    assert out[0]["SUBJECT_GROUP"] == "Drug 10mg"                # from IRT, not RAND.ARM ('Masked')


def test_subject_consent(built):
    out = rows(built, "SUBJECT_CONSENT", "32703", ("PROTOCOL_AMENDMENT_NUMBER",))
    assert [(r["SUBJECT"], r["PROTOCOL_AMENDMENT_NUMBER"], r["CONSENT_DATE"]) for r in out] == [
        ("101-01-001", "1.0", "2025-01-02"),   # earliest of IC v2.0 / ICR 2 ; ICO excluded
        ("101-01-001", "2.0", "2025-04-01"),
    ]
    assert {(r["CONSENT_SIGNED_YN"], r["CONSENT_TYPE"]) for r in out} == {("Yes", "Main Study")}


def test_subject_visit(built):
    out = rows(built, "SUBJECT_VISIT", "32703", ("_SOURCE_ENTITY", "VISIT_NAME"))
    got = [(r["VISIT_NAME"], r["SAMPLE_TRACKING_CODE"], r["COLLECTION_COMPLETED"], r["MADE_VISIT"],
            r["VISIT_DATE"]) for r in out]
    assert got == [
        ("Day 1 of Cycle 4", "Pre-dose", "Yes", "Yes", "2025-04-01"),
        ("Unscheduled Visit", "Other : Frozen", "No", "No", "2025-04-20"),   # matched on date
        ("Day 1 of Cycle 4 2h post-dose", "PK Serum", "Yes", "Yes", "2025-04-01"),
    ]
    assert {r["ACCESSION_ID"] for r in out} == {""}


def test_study_details_and_sites(built):
    details = rows(built, "STUDY_DETAILS", "32703", ("STUDY_NUMBER",))
    assert [(d["STUDY_COMPOUND"], d["FSFV"], d["PLANNED_ENROLLMENT"]) for d in details] == \
        [("BNT327", "2024-11-01", "120")]
    sites = rows(built, "STUDY_SITES", "32703", ("SITE_NUMBER",))
    assert [(s["SITE_NUMBER"], s["SITE_NAME"], s["FSFV"]) for s in sites] == \
        [("101-01", "Site One", "2024-12-01")]


def test_rerun_one_study_leaves_others_untouched(built, cfg):
    before = built.table(f"{GOLD}.SUBJECT_DETAILS").groupBy("STUDYID").count().collect()
    run(built, cfg, GOLD, studies=["32703"], fmt="parquet")
    after = built.table(f"{GOLD}.SUBJECT_DETAILS").groupBy("STUDYID").count().collect()
    assert sorted(map(tuple, before)) == sorted(map(tuple, after)) == [("32703", 4), ("99901", 2)]
