# B-SMART silver → gold (reusable, mapping-table driven)

Right now there is separate code for each study. This repo has **one pipeline for all studies**.
What gets built comes from a mapping table, so onboarding a study is a config change, not new code.

```
silver (CDR / ODR)  ──►  mapping table + entity config + study config  ──►  generic pipeline  ──►  gold
```

## The mapping table — `config/mapping_table.csv`

| Column | Meaning |
|---|---|
| `STUDYID` | `DEFAULT` = applies to every study; a real STUDYID (e.g. `32703`) overrides the DEFAULT row with the same `ENTITY_NAME` + `TARGET_COLUMN_NAME` |
| `ENTITY_NAME` | Loader entity, e.g. `SUBJECT_DETAILS_SCREENING` |
| `MAPPING_TABLE_NAME` | Silver table alias (e.g. `DM`, `RAND`, `IC`) |
| `MAPPING_COLUMN_NAME` | Source column(s), e.g. `RACE_WHITE, RACE_ASIAN, …` |
| `ETL_LOGIC` | Spark SQL expression. Leave it empty for a straight copy of `TABLE.COLUMN` |
| `TARGET_COLUMN_NAME` | Gold column name |
| `COLUMN_ORDER`, `DATA_TYPE`, `IS_ACTIVE` | Column order, cast type (default `STRING`), `N` disables the row |

The first five columns are the ones from the original screenshot. The last ones were added
because the pipeline needs them to run: without `TARGET_COLUMN_NAME`, for example, `RACE_WHITE`
could not land in `RACE`.

Examples (from the template's *Subject Details* sheet):

| STUDYID | ENTITY_NAME | MAPPING_TABLE_NAME | MAPPING_COLUMN_NAME | ETL_LOGIC (shortened) | TARGET |
|---|---|---|---|---|---|
| DEFAULT | SUBJECT_DETAILS_SCREENING | DM | PROJECT | | STUDY_NUMBER |
| DEFAULT | SUBJECT_DETAILS_SCREENING | DM | SUBJECT | | SUBJECT |
| DEFAULT | SUBJECT_DETAILS_SCREENING | DM | RACE_WHITE, RACE_ASIAN, … | 1 race → its label, several → `'Mixed : ' + comma-separated list` | RACE |
| DEFAULT | SUBJECT_DETAILS_RANDOMIZED | RAND | DSSTDAT_RAW | `date_format(to_date(RAND.DSSTDAT_RAW,'d MMM yyyy'),'yyyy-MM-dd')` | RANDOMIZATION_DATE |

Two more small tables complete the config:

* `config/entity_config.csv`: per entity, the gold table it feeds, `FROM_SQL` (driver table + joins),
  `WHERE_SQL`, and optional dedup (`DEDUP_KEYS`, `DEDUP_ORDER_BY`). Entities that share a
  `GOLD_TABLE` are unioned. For example, the screening and randomized Subject Details records
  are unioned, and so are the BE and PC Subject Visit records.
* `config/study_config.csv`: one row per study. Every column can be used as a `${NAME}`
  placeholder in SQL, e.g. `${CDR_SCHEMA}`, `${STUDY_NUMBER}`, `${BLINDING_TYPE}`,
  `${FIRST_DOSE_TREATMENT}`. An unknown placeholder fails the run.

`config/B-SMART_Mapping_Table.xlsx` is an Excel view of the same config, with a `RESOLVED_<STUDYID>`
sheet showing the effective mapping per study. Regenerate it with
`python scripts/export_mapping_xlsx.py`. **The CSVs are the source of truth.**

## Entities covered (from `B-SMART_Mapping_Template_V3_2.xlsx`)

| Gold table | Entities | Source |
|---|---|---|
| SUBJECT_DETAILS | SUBJECT_DETAILS_SCREENING, SUBJECT_DETAILS_RANDOMIZED | CDR DM, RAND, DD, DSEOS, EX, SV + ODR participant status |
| SUBJECT_CONSENT | SUBJECT_CONSENT | CDR IC (IC + ICR, first per subject/amendment) |
| SUBJECT_VISIT | SUBJECT_VISIT_BE, SUBJECT_VISIT_PC | CDR BE / PC left joined to SV |
| STUDY_DETAILS | STUDY_DETAILS | ODR studies |
| STUDY_SITES | STUDY_SITES | ODR trial_site |

The Vendor (biomaterial DTS) sheets and the QC Checks sheet are not implemented yet.

## Onboarding a new study

1. Add a row to `study_config.csv`.
2. Only where the study differs from the standard, add rows with that STUDYID to
   `mapping_table.csv` / `entity_config.csv`. For example, a blinded study takes Subject Group
   from IRT instead of `RAND.ARM`; `tests/conftest.py` shows exactly that override.
3. Run the pipeline. Nothing else changes.

## Running

Databricks: open `notebooks/bsmart_gold_pipeline.py`, set the widgets (`studies`, `gold_schema`,
optional `config_source` if the config lives in Delta tables), and run it.

CLI:

```bash
python -m bsmart_gold --study 32703 --dry-run               # print generated SQL
python -m bsmart_gold --gold-schema gold.bsmart             # all active studies
python -m bsmart_gold --gold-schema gold.bsmart --study 32703
```

Each run replaces only that study's rows in each gold table (Delta `replaceWhere STUDYID = …`),
so studies can be rerun independently. Gold rows carry `STUDYID`, `_SOURCE_ENTITY` and `_LOAD_TS`.

## Tests

```bash
pip install -e ".[dev]"
pytest
```

The tests build fake silver tables for two studies, an open-label one (`32703`) and a blinded one
with overrides (`99901`). They run the shipped mapping end to end and check the business rules:
race concatenation, mortality blanked for blinded studies, first-dose filter, enrollment-status
priority, consent dedup, visit-name cleanup, and unscheduled-visit date matching. They also check
that rerunning one study leaves other studies' rows untouched.

## To confirm against the real silver layer

These came from the template, but they still need checking against the actual tables:

* **Study 32703 values:** `STUDY_NUMBER = BNT327-03` (inferred from the BNT327 treatment name in
  the template), `BLINDING_TYPE = OPEN LABEL`, and the schemas `silver.cdr` / `silver.odr`.
* **ODR participant status:** the table and column names (`PARTICIPANT_STATUS`, `SUBJECT`,
  `PARTICIPANT_STATUS`, `PARTICIPANT_STATUS_DATE`) are placeholders, because the template does not
  name them.
* **ODR study columns:** names are copied from the template as written (e.g.
  `FIRSTPARTCIPANTFIRSTVISIT_ACTUALDATE`, `TRAILEND_ACTUALDATE`). LSLV uses
  `LASTPARTCIPANTLASTVISIT_ACTUALDATE`, because the template's `…FIRSTVISIT…` looks like a typo.
* **Date formats:** EDC `*_RAW` dates are parsed as `d MMM yyyy`, as in the template's consent
  logic. `SV.VISDAT`, `BEDAT`, `PCDAT` and ODR dates are assumed to already be dates or ISO strings.
* **RAND:** assumed to hold one row per subject. The randomized flag column is assumed to be
  `RANDYN`, though the template also mentions `RANYN`.
