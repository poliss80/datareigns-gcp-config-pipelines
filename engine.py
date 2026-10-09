#!/usr/bin/env python3
"""Config driven batch ingestion engine.

Reads one YAML pipeline config and executes it: read source files,
enforce schema, run validations, apply transforms, dedup, load into
SQLite (a local stand in for BigQuery), and write one audit row.

Usage:
    python engine.py configs/sales_daily_csv.yaml
    python engine.py configs/customer_dim.yaml --source data/customers_update.csv
    python engine.py configs/sales_daily_csv.yaml --db /tmp/warehouse.db
"""

import argparse
import ast
import csv
import json
import operator
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path

import yaml

BASE = Path(__file__).parent


# ---------------------------------------------------------------------------
# Safe formula evaluation for derive transforms.
# Only column names, numbers, arithmetic, comparisons, and a small set of
# text functions are allowed. No attribute access, no imports, no calls
# outside the whitelist.
# ---------------------------------------------------------------------------
_BINOPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARYOPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}
_CMPOPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}


def _safe_func(name, args):
    if name == "substr" and len(args) == 3:
        s, a, b = args
        return None if s is None else str(s)[int(a) : int(a) + int(b)]
    if name == "upper" and len(args) == 1:
        return None if args[0] is None else str(args[0]).upper()
    if name == "lower" and len(args) == 1:
        return None if args[0] is None else str(args[0]).lower()
    raise ValueError(f"unsupported function: {name}")


def _eval(node, row):
    if isinstance(node, ast.Expression):
        return _eval(node.body, row)
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in row:
            return row[node.id]
        raise ValueError(f"unknown column in formula: {node.id}")
    if isinstance(node, ast.BinOp) and type(node.op) in _BINOPS:
        left = _eval(node.left, row)
        right = _eval(node.right, row)
        if left is None or right is None:
            return None
        return _BINOPS[type(node.op)](left, right)
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARYOPS:
        val = _eval(node.operand, row)
        return None if val is None else _UNARYOPS[type(node.op)](val)
    if isinstance(node, ast.BoolOp):
        vals = [_eval(v, row) for v in node.values]
        return all(vals) if isinstance(node.op, ast.And) else any(vals)
    if (
        isinstance(node, ast.Compare)
        and len(node.ops) == 1
        and type(node.ops[0]) in _CMPOPS
    ):
        left = _eval(node.left, row)
        right = _eval(node.comparators[0], row)
        if left is None or right is None:
            return None
        return _CMPOPS[type(node.ops[0])](left, right)
    if isinstance(node, ast.IfExp):
        return _eval(node.body, row) if _eval(node.test, row) else _eval(node.orelse, row)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and not node.keywords:
        return _safe_func(node.func.id, [_eval(a, row) for a in node.args])
    raise ValueError(f"unsupported formula element: {ast.dump(node)}")


def eval_formula(formula, row):
    return _eval(ast.parse(formula, mode="eval"), row)


# ---------------------------------------------------------------------------
# Type coercion and validation
# ---------------------------------------------------------------------------
_DATE_FORMATS = ("%Y-%m-%d", "%Y/%m/%d")


def coerce(value, typ, col):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        if typ == "int":
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, float):
                if not value.is_integer():
                    raise ValueError("not a whole number")
                return int(value)
            return int(str(value).strip())
        if typ == "float":
            return float(value)
        if typ == "str":
            return str(value)
        if typ == "date":
            if isinstance(value, (datetime, date)):
                return value.isoformat()[:10]
            text = str(value).strip()
            for fmt in _DATE_FORMATS:
                try:
                    return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            raise ValueError(f"bad date: {text}")
        if typ == "bool":
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in ("true", "1", "yes"):
                return True
            if text in ("false", "0", "no"):
                return False
            raise ValueError(f"bad bool: {text}")
    except (ValueError, TypeError) as exc:
        raise ValueError(f"column {col}: cannot cast {value!r} to {typ}") from exc
    raise ValueError(f"unknown type: {typ}")


def check_rule(value, rule):
    col = rule["column"]
    kind = rule["rule"]
    if kind == "not_null":
        return None if value is not None else f"{col} is null"
    if value is None:
        return None
    target = rule.get("value")
    if kind == "gte" and not value >= target:
        return f"{col}={value} below minimum {target}"
    if kind == "gt" and not value > target:
        return f"{col}={value} not above {target}"
    if kind == "lte" and not value <= target:
        return f"{col}={value} above maximum {target}"
    if kind == "lt" and not value < target:
        return f"{col}={value} not below {target}"
    if kind == "in" and value not in target:
        return f"{col}={value} not in allowed set"
    return None


def process_row(raw, schema, validations, transforms):
    row = {}
    errors = []
    for field in schema:
        name = field["name"]
        typ = field["type"]
        required = field.get("required", False)
        try:
            val = coerce(raw.get(name), typ, name)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if required and val is None:
            errors.append(f"{name} is required but missing")
        row[name] = val
    if errors:
        return None, errors
    for rule in validations or []:
        err = check_rule(row.get(rule["column"]), rule)
        if err:
            errors.append(err)
    if errors:
        return None, errors
    for step in transforms or []:
        op = step["op"]
        if op == "rename":
            if step["from"] in row:
                row[step["to"]] = row.pop(step["from"])
        elif op == "cast":
            try:
                row[step["column"]] = coerce(row.get(step["column"]), step["type"], step["column"])
            except ValueError as exc:
                errors.append(str(exc))
        elif op == "derive":
            try:
                row[step["column"]] = eval_formula(step["formula"], row)
            except Exception as exc:
                errors.append(f"derive {step['column']}: {exc}")
        elif op == "drop":
            row.pop(step["column"], None)
        else:
            errors.append(f"unknown transform op: {op}")
    if errors:
        return None, errors
    return row, []


# ---------------------------------------------------------------------------
# Source readers
# ---------------------------------------------------------------------------
def read_csv(path, delimiter=","):
    with open(path, newline="", encoding="utf-8") as fh:
        yield from csv.DictReader(fh, delimiter=delimiter)


def read_json(path):
    text = Path(path).read_text(encoding="utf-8").strip()
    if not text:
        return
    if text.startswith("["):
        yield from json.loads(text)
    else:
        for line in text.splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)


def read_source(source_cfg, override):
    if override:
        paths = [override]
    else:
        declared = source_cfg["path"]
        paths = [declared] if isinstance(declared, str) else list(declared)
    fmt = source_cfg.get("format", "csv")
    for path in paths:
        if fmt == "csv":
            yield from read_csv(path, source_cfg.get("delimiter", ","))
        elif fmt == "json":
            yield from read_json(path)
        else:
            raise ValueError(f"unsupported source format: {fmt}")


# ---------------------------------------------------------------------------
# SQLite loading (stand in for BigQuery)
# ---------------------------------------------------------------------------
def sqlite_type(value):
    if isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    return "TEXT"


def ensure_table(con, table, rows, merge_keys):
    cols = list(rows[0].keys())
    coldefs = ", ".join(f'"{c}" {sqlite_type(rows[0][c])}' for c in cols)
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({coldefs})')
    if merge_keys:
        ux = f"ux_{table}_" + "_".join(merge_keys)
        keylist = ", ".join(f'"{c}"' for c in merge_keys)
        con.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{ux}" ON "{table}" ({keylist})')


def insert_rows(con, table, rows):
    cols = list(rows[0].keys())
    names = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" * len(cols))
    con.executemany(
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders})',
        [[r.get(c) for c in cols] for r in rows],
    )


def load_append(con, table, rows, partition_by):
    partitions = []
    if partition_by:
        partitions = sorted({str(r[partition_by]) for r in rows if r.get(partition_by) is not None})
        for part in partitions:
            con.execute(f'DELETE FROM "{table}" WHERE "{partition_by}" = ?', (part,))
    insert_rows(con, table, rows)
    return partitions


def load_merge(con, table, rows, merge_keys):
    cols = list(rows[0].keys())
    names = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" * len(cols))
    keylist = ", ".join(f'"{c}"' for c in merge_keys)
    updates = ", ".join(f'"{c}" = excluded."{c}"' for c in cols if c not in merge_keys)
    con.executemany(
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) '
        f"ON CONFLICT({keylist}) DO UPDATE SET {updates}",
        [[r.get(c) for c in cols] for r in rows],
    )


def ensure_audit(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS pipeline_audit (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT,
            started_at TEXT,
            finished_at TEXT,
            duration_sec REAL,
            rows_read INTEGER,
            rows_loaded INTEGER,
            rows_rejected INTEGER,
            rows_quarantined INTEGER,
            rows_deduped INTEGER,
            destination TEXT,
            load_mode TEXT,
            partitions TEXT,
            status TEXT,
            message TEXT
        )"""
    )


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------
def load_config(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def run_pipeline(config_path, source_override=None, db_path="warehouse.db"):
    started_at = datetime.now().isoformat(timespec="seconds")
    start = time.time()
    cfg = load_config(config_path)
    name = cfg["pipeline"]
    dest = cfg.get("destination", {})
    table = dest.get("table", "")
    mode = dest.get("mode", "append")
    stats = {
        "rows_read": 0,
        "rows_loaded": 0,
        "rows_rejected": 0,
        "rows_quarantined": 0,
        "rows_deduped": 0,
    }
    partitions = ""
    status, message = "ok", ""
    con = sqlite3.connect(db_path)
    try:
        ensure_audit(con)
        qtable = (cfg.get("quarantine") or {}).get("table")
        raw_rows = list(read_source(cfg["source"], source_override))
        stats["rows_read"] = len(raw_rows)
        good, bad = [], []
        for raw in raw_rows:
            clean, errs = process_row(
                raw, cfg.get("schema", []), cfg.get("validations", []), cfg.get("transforms", [])
            )
            if clean is None:
                bad.append((raw, errs))
            else:
                good.append(clean)
        stats["rows_rejected"] = len(bad)
        if bad and qtable:
            qrows = []
            for raw, errs in bad:
                qrow = {"_error": "; ".join(errs)}
                for key, val in raw.items():
                    qrow[f"src_{key}"] = val
                qrows.append(qrow)
            ensure_table(con, qtable, qrows, None)
            insert_rows(con, qtable, qrows)
            stats["rows_quarantined"] = len(qrows)
        keys = (cfg.get("dedup") or {}).get("keys", [])
        if keys and good:
            before = len(good)
            seen = {}
            for row in good:
                seen[tuple(row.get(k) for k in keys)] = row
            good = list(seen.values())
            stats["rows_deduped"] = before - len(good)
        if good:
            ensure_table(con, table, good, dest.get("merge_keys"))
            if mode == "merge":
                load_merge(con, table, good, dest["merge_keys"])
            else:
                parts = load_append(con, table, good, dest.get("partition_by"))
                partitions = ",".join(parts)
        stats["rows_loaded"] = len(good)
        con.commit()
    except Exception as exc:
        con.rollback()
        status = "failed"
        message = str(exc)[:500]
        raise
    finally:
        duration = time.time() - start
        try:
            con.execute(
                """INSERT INTO pipeline_audit
                   (pipeline, started_at, finished_at, duration_sec, rows_read,
                    rows_loaded, rows_rejected, rows_quarantined, rows_deduped,
                    destination, load_mode, partitions, status, message)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    name,
                    started_at,
                    datetime.now().isoformat(timespec="seconds"),
                    round(duration, 2),
                    stats["rows_read"],
                    stats["rows_loaded"],
                    stats["rows_rejected"],
                    stats["rows_quarantined"],
                    stats["rows_deduped"],
                    table,
                    mode,
                    partitions,
                    status,
                    message,
                ),
            )
            con.commit()
        finally:
            con.close()
    print(
        f"[{name}] status={status} "
        f"read={stats['rows_read']} loaded={stats['rows_loaded']} "
        f"rejected={stats['rows_rejected']} quarantined={stats['rows_quarantined']} "
        f"deduped={stats['rows_deduped']} dest={table} mode={mode} "
        f"partitions={partitions or 'none'} secs={duration:.2f}"
    )
    return {"pipeline": name, "status": status, **stats}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Config driven batch ingestion engine.")
    parser.add_argument("config", help="Path to pipeline YAML config")
    parser.add_argument("--source", help="Override the source file path from the config")
    parser.add_argument("--db", default="warehouse.db", help="SQLite database path")
    args = parser.parse_args(argv)
    try:
        run_pipeline(args.config, source_override=args.source, db_path=args.db)
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
