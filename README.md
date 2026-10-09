# Config Driven Batch Ingestion for Google Cloud

One engine, many YAML configs. A portfolio demo by DataReigns.

Built with Python 3.12 stdlib plus pyyaml. No cloud credentials needed.

## The problem this solves

Every new data source brings the same toil: read the files, coerce the
types, reject the bad rows, shape the columns, dedup, load, and prove
what happened. Teams rebuild this glue for every pipeline, and every
rebuild drifts a little. This project shows a better shape: one engine
that never changes, plus small YAML configs that describe each pipeline.
A new source means a new config, not new code.

## Architecture

    =====================================================
    YAML pipeline configs (what to load and how)
    =====================================================
                            |
                            v
    =====================================================
    engine.py
      read source files
      enforce schema and validations
      apply transforms (rename, cast, derive)
      dedup on keys
      load append or merge into SQLite
      write one audit row per run
    =====================================================
            |                              |
            v                              v
    =================            =====================
    SQLite tables                pipeline_audit table
    (stand in for                rows in, rows out,
     BigQuery)                   rejects, duration
    =================            =====================

SQLite stands in for BigQuery so the whole thing runs on a laptop.

## Quickstart

Install the one dependency and generate the sample data:

    pip install pyyaml
    python data/generate_data.py

Run the three pipelines:

    python engine.py configs/sales_daily_csv.yaml
    python engine.py configs/clickstream_json.yaml
    python engine.py configs/customer_dim.yaml

Each run prints a one line summary and appends a row to the
pipeline_audit table.

To see merge in action, open configs/customer_dim.yaml, change
source.path to data/customers_update.csv, and run the engine again.
Updated customers change in place and new customers appear.

Then inspect everything:

    python inspect.py

## The three sample pipelines

* sales_daily: two daily sales CSV files appended into sales_daily,
  partitioned by sale date. Shows schema coercion, rename transforms,
  a derived total column, dedup on order id, and partition overwrite.
  Run it twice and the row count stays the same.
* clickstream_events: a JSON event file with schema validation. Bad
  rows land in clickstream_quarantine with an _error column explaining
  each rejection instead of vanishing silently.
* customer_dim: a customer CSV merged into customer_dim on customer
  id. Run it once, then point it at customers_update.csv to watch
  updates apply and new customers appear.

## Config field reference

Each pipeline is one YAML file. Fields:

* pipeline: name used in the audit log
* description: human readable summary
* source.path: one file path or a list of file paths
* source.format: csv or json. JSON may be an array in one file or one
  object per line.
* source.delimiter: CSV delimiter, defaults to a comma
* schema: fields with name, type (int, float, str, date, bool) and
  required (true or false)
* validations: rules with column, rule, and value. Rules: not_null,
  gt, gte, lt, lte, in
* transforms: steps applied in order. rename needs from and to. cast
  needs column and type. derive needs column and formula. drop needs
  column. Formulas accept column names, numbers, arithmetic,
  comparisons, and the functions substr, upper, lower. Quote any
  formula that contains a comma so YAML does not split it.
* dedup.keys: columns that define a duplicate row. The last row wins.
* destination.table: SQLite table to write
* destination.mode: append or merge
* destination.partition_by: with append, rows for incoming partition
  values replace existing rows first
* destination.merge_keys: with merge, the unique key columns
* quarantine.table: rejected rows are written here with an _error
  column. Without it, rejects are only counted.

## Production mapping: from laptop to Google Cloud

This demo runs locally on purpose. Each piece maps directly to a
production GCP component:

* Local CSV and JSON files become objects in a Cloud Storage bucket.
  Only the source path changes, to a gs:// URI. The engine code is
  untouched.
* SQLite tables become BigQuery tables. partition_by becomes the
  BigQuery partition column. Append mode becomes a BigQuery append
  load. Merge mode becomes a BigQuery MERGE statement on the merge
  keys.
* engine.py moves onto a schedule. One Cloud Composer DAG task per
  config, or one Cloud Run job per config triggered by Cloud
  Scheduler.
* Transforms declared in YAML become dbt models: rename and cast
  become staging models, derive becomes documented SQL, validations
  become dbt tests.
* pipeline_audit becomes a BigQuery audit dataset. Add a Cloud
  Monitoring alert when rejected rows spike, and you have data
  quality SLAs.
* Quarantine tables become a BigQuery quarantine dataset that the
  data team reviews and reprocesses.

Nothing about the config format changes between the laptop and
production. That is the point.
