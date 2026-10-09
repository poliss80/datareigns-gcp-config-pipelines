#!/usr/bin/env python3
"""Print row counts and the audit log from warehouse.db."""
import sqlite3

con = sqlite3.connect("warehouse.db")
print("TABLE COUNTS")
for table in [
    "sales_daily",
    "clickstream_events",
    "clickstream_quarantine",
    "customer_dim",
    "pipeline_audit",
]:
    try:
        n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        n = "missing"
    print(f"  {table}: {n}")
print("AUDIT LOG")
for row in con.execute(
    "SELECT pipeline, rows_read, rows_loaded, rows_rejected, "
    "rows_quarantined, rows_deduped, status "
    "FROM pipeline_audit ORDER BY run_id"
):
    print(" ", row)
