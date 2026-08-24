#!/usr/bin/env python3
"""Run the HCPCS category distribution query through DuckDB's EXPLAIN ANALYZE.

Shells out to the duckdb CLI (no python duckdb package is installed here) and
streams its output straight to the terminal.
"""

import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DATA_ROOT = REPO_ROOT / "data"

# Resolved query as provided, with the "/mnt/..." parquet path swapped for
# the local data/ layout (same folder structure, different mount).
QUERY = r"""
SELECT "HCPCS Category Level 1" AS "HCPCS Category Level 1", SUM("Claim Count")/SUM("Total Claim Count (All)") AS "Claim Distribution (% of whole)"
FROM (
WITH base_claim_population AS (
  SELECT
    p."CLM_ID",
    p."CLM_PAID_AMT",
    p."bitmap_hcpcs_cat_l1",
    p."nedl_mrr"                         AS mrr
  FROM read_parquet(['__DATA_ROOT__/fs_priced/*.parquet'], union_by_name = TRUE) p
  WHERE
    p."nedl_original_claim" = TRUE
    AND p."nedl_total_payment" > 0

    AND p."BUS_SGMT" IN ('MA', 'EXH', 'INDV', 'L001', 'L002', 'M001', 'M002', 'S001', 'STUD')

    AND p."CLAIM_IN_NETWORK" IN ('Y')

    AND p."nedl_thru_date" >= make_date(
          CAST(SUBSTR('2020-Q1', 1, 4) AS INTEGER),
          1 + (CAST(SUBSTR('2020-Q1', -1, 1) AS INTEGER) - 1) * 3,
          1
        )
    AND p."nedl_thru_date" < make_date(
          CAST(SUBSTR('2026-Q2', 1, 4) AS INTEGER)
            + CASE WHEN CAST(SUBSTR('2026-Q2', -1, 1) AS INTEGER) = 4 THEN 1 ELSE 0 END,
          CASE
            WHEN CAST(SUBSTR('2026-Q2', -1, 1) AS INTEGER) = 4 THEN 1
            ELSE 1 + CAST(SUBSTR('2026-Q2', -1, 1) AS INTEGER) * 3
          END,
          1
        )

    AND p."bitmap_fs_types" = 16
),

population_mrr_stats AS (
  SELECT
    AVG(b.mrr)        AS mean_mrr,
    STDDEV_POP(b.mrr) AS std_mrr,
    COUNT(*)          AS pop_cnt
  FROM base_claim_population b
),

filtered_claim_population AS (
  SELECT
    b.*,
    s.mean_mrr,
    s.std_mrr,
    s.pop_cnt,
    ABS((b.mrr - s.mean_mrr) / s.std_mrr) AS z_abs
  FROM base_claim_population b
  CROSS JOIN population_mrr_stats s
  WHERE
    TRUE
    AND b."CLM_PAID_AMT" >= 1

    AND b.mrr >= 1

    AND pop_cnt >= 2
    AND std_mrr > 0
    AND z_abs >= 2
),

total_claims_all AS (
  SELECT
    COUNT(*) AS "Total Claim Count (All)"
  FROM filtered_claim_population
  WHERE ("bitmap_hcpcs_cat_l1" & 65535) != 0
),

claim_hcpcs_category_map AS (
  SELECT
    f.mrr,
    d.hcpcs_category_l1
  FROM filtered_claim_population f
  INNER JOIN (
    SELECT * FROM (VALUES
      (1,     'Anesthesia'),
      (2,     'Surgery'),
      (4,     'Radiology Procedures'),
      (8,     'Pathology and Laboratory Procedures'),
      (16,    'Evaluation and Management'),
      (32,    'Medicine Services and Procedures'),
      (64,    'Ambulance and Other Transport Services'),
      (128,   'Medical and Surgical Supplies'),
      (256,   'Enteral and Parenteral Therapy'),
      (512,   'Outpatient PPS'),
      (1024,  'Durable Medical Equipment (DME)'),
      (2048,  'Procedures / Professional Services'),
      (4096,  'Drugs (Non-Oral / Chemo)'),
      (8192,  'Orthotic / Prosthetic Procedures'),
      (16384, 'Temporary Codes'),
      (32768, 'Vision Services')
    ) AS v(bit, hcpcs_category_l1)
  ) d
    ON (f."bitmap_hcpcs_cat_l1" & d.bit) != 0
)

SELECT
  hcpcs_category_l1             AS "HCPCS Category Level 1",
  COUNT(*)                      AS "Claim Count",
  SUM(mrr) / 100.0              AS "Total Medicare Relativity",
  t."Total Claim Count (All)"   AS "Total Claim Count (All)"
FROM claim_hcpcs_category_map
CROSS JOIN total_claims_all t
GROUP BY
  hcpcs_category_l1,
  t."Total Claim Count (All)"
) AS virtual_table
GROUP BY "HCPCS Category Level 1"
ORDER BY "Claim Distribution (% of whole)" DESC
LIMIT 5;
"""


def main() -> int:
    duckdb_bin = shutil.which("duckdb") or "/usr/local/bin/duckdb"
    if not Path(duckdb_bin).exists():
        print(f"duckdb CLI not found at {duckdb_bin}", file=sys.stderr)
        return 1

    query = QUERY.replace("__DATA_ROOT__", DATA_ROOT.as_posix())
    query = "EXPLAIN ANALYZE\n" + query

    result = subprocess.run([duckdb_bin], input=query, text=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
