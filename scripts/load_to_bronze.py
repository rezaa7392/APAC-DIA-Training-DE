from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pathlib
from typing import List, Dict, Optional

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from deltalake import write_deltalake 



# ----------------------- CLI & dirs -----------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=str, default="data_raw", help="Raw data root folder")
    ap.add_argument("--lake", type=str, default="lake", help="Lake root folder")
    ap.add_argument("--manifest", type=str, default="duckdb/warehouse.duckdb", help="DuckDB file for manifest")
    return ap.parse_args()


def ensure_dirs(lake_root: pathlib.Path):
    (lake_root / "bronze" / "parquet").mkdir(parents=True, exist_ok=True)
    (lake_root / "bronze" / "delta").mkdir(parents=True, exist_ok=True)
    (lake_root / "_rejects").mkdir(parents=True, exist_ok=True)


# ----------------------- Schema import -----------------------
def load_arrow_schemas() -> Dict[str, pa.Schema]:
    """Import pyarrow.Schema objects from schemas/schemas.py (preferred) or schemas.py (fallback)."""
    from importlib import import_module, util
    mod = None
    try:
        mod = import_module("schemas.schemas")
    except Exception:
        try:
            mod = import_module("schemas")
        except Exception:
            for cand in ("schemas/schemas.py", "schemas.py"):
                p = pathlib.Path(cand)
                if p.exists():
                    spec = util.spec_from_file_location("schemas_dynamic", str(p))
                    m = util.module_from_spec(spec)
                    assert spec and spec.loader
                    spec.loader.exec_module(m)  # type: ignore[attr-defined]
                    mod = m
                    break
    if mod is None:
        raise RuntimeError("Cannot import schemas/schemas.py. Run from repo root.")

    out: Dict[str, pa.Schema] = {}
    for name in dir(mod):
        obj = getattr(mod, name)
        if isinstance(obj, pa.Schema):
            key = name[:-7] if name.endswith("_schema") else name
            out[key] = obj
    if not out:
        raise RuntimeError("No pyarrow.Schema objects found in schemas/schemas.py")
    return out


# ----------------------- Manifest (DuckDB) -----------------------
def init_manifest(conn: duckdb.DuckDBPyConnection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            src_md5  TEXT,
            processed_at TIMESTAMP,
            row_count BIGINT
        )
        """
    )


def file_md5(path: pathlib.Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def already_processed(conn: duckdb.DuckDBPyConnection, p: pathlib.Path) -> bool:
    row = conn.execute(
        "SELECT src_md5 FROM manifest_processed_files WHERE src_path = ?", [str(p)]
    ).fetchone()
    if row is None:
        return False
    try:
        return row[0] == file_md5(p)
    except Exception:
        return False


def mark_processed(conn: duckdb.DuckDBPyConnection, p: pathlib.Path, nrows: int):
    conn.execute(
        "INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?, ?)",
        [str(p), file_md5(p), pd.Timestamp.now(tz="UTC").to_pydatetime(), int(nrows)],
    )


# ----------------------- Hive helpers -----------------------
def _parse_hive_parts(path: pathlib.Path, keys: List[str]) -> Dict[str, Optional[str]]:
    """Extract hive style partition values from path parents (e.g., dt=YYYY-MM-DD)."""
    vals: Dict[str, Optional[str]] = {k: None for k in keys}
    cur = path.parent
    while cur and cur != cur.parent:
        name = cur.name
        if "=" in name:
            k, v = name.split("=", 1)
            if k in vals and vals[k] is None:
                vals[k] = v
        cur = cur.parent
    return vals


def _append_partition_cols(tbl: pa.Table, parts: Dict[str, Optional[str]]) -> pa.Table:
    for col, val in parts.items():
        if col in tbl.column_names:
            continue
        arr = pa.array([val] * tbl.num_rows, type=pa.string())
        tbl = tbl.append_column(col, arr)
    return tbl


def _derived_field_defs(cols: List[str], types: Optional[Dict[str, pa.DataType]]) -> List[pa.Field]:
    types = types or {}
    return [pa.field(c, types.get(c, pa.string())) for c in cols]


# ----------------------- Hash helper -----------------------
def _hash_row_md5(values) -> str:
    m = hashlib.md5()
    m.update(
        "|".join("" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v) for v in values).encode("utf-8")
    )
    return m.hexdigest()


# ----------------------- Parquet / Delta writers -----------------------
def write_parquet_partitioned(tbl: pa.Table, base_path: pathlib.Path, partitioning: Optional[List[str]]):
    base_path.mkdir(parents=True, exist_ok=True)
    if not partitioning:
        out = base_path / f"part-{hashlib.md5(str(pd.Timestamp.now(tz='UTC')).encode()).hexdigest()[:8]}.parquet"
        pq.write_table(tbl, out, compression="snappy")
        return
    con = duckdb.connect()
    con.register("t", tbl)
    part_cols = ", ".join([f'"{c}"' for c in partitioning])
    con.execute(
        f"COPY t TO '{base_path.as_posix()}' (FORMAT PARQUET, PARTITION_BY ({part_cols}), OVERWRITE_OR_IGNORE TRUE)"
    )
    con.close()


def write_delta(
    tbl: pa.Table,
    base_path: pathlib.Path,
    *,
    mode: str = "append",
    partition_by: Optional[List[str]] = None,
    schema_mode: Optional[str] = "merge",
) -> bool:
    if write_deltalake is None:
        # Raise to make it visible (Bronze spec wants dual outputs if available)
        raise RuntimeError("deltalake is not installed. pip install 'deltalake>=0.18.0'")
    base_path.mkdir(parents=True, exist_ok=True)
    # Safety: partition columns must exist to create a partitioned table
    if partition_by:
        missing = [c for c in (partition_by if isinstance(partition_by, list) else [partition_by]) if c not in tbl.column_names]
        if missing:
            raise ValueError(f"Delta write: partition column(s) {missing} not found in data")
    write_deltalake(
        str(base_path),
        data=tbl,
        partition_by=partition_by,
        mode=mode,
        schema_mode=schema_mode,
    )
    return True


# ----------------------- Strict row validation (no repair) -----------------------
def _validate_to_typed_and_rejects(df_raw: pd.DataFrame, schema: pa.Schema) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Validate df_raw (strings) against schema:
       - parse to typed df (numbers/dates/bools); do NOT "fix" values, just parse or NA
       - build rejects for any row with type parse failure (non-blank that couldn't parse)
         or required NULL in non-nullable
       - return (clean_typed_df, rejects_raw_df_with_reason)
    """
    schema_cols = [f.name for f in schema]
    df_raw = df_raw.copy()
    # Retain only schema columns; add missing as NA; normalize to string dtype for inspection
    for c in schema_cols:
        if c not in df_raw.columns:
            df_raw[c] = pd.NA
    df_raw = df_raw[schema_cols].astype("string")

    df_typed = df_raw.copy()
    type_fail = pd.DataFrame(False, index=df_raw.index, columns=schema_cols)
    null_fail = pd.DataFrame(False, index=df_raw.index, columns=schema_cols)

    def _is_blank(series: pd.Series) -> pd.Series:
        return series.isna() | (series.str.strip() == "")

    for fld in schema:
        col = fld.name
        t = fld.type
        s = df_raw[col]
        blank = _is_blank(s)

        if pa.types.is_integer(t) or pa.types.is_floating(t) or pa.types.is_decimal(t):
            parsed = pd.to_numeric(s.where(~blank, None), errors="coerce")
            type_fail[col] = (~blank) & parsed.isna()
            df_typed[col] = parsed
        elif pa.types.is_date(t) or pa.types.is_timestamp(t):
            raw = s.where(~blank, None)

            # Pass 1: general parse (dayfirst) — catches most dd/mm/YYYY
            parsed = pd.to_datetime(raw, errors="coerce", dayfirst=True, utc=False)

            # Pass 2: explicit dd/mm/YYYY for any still NaT
            mask2 = parsed.isna() & (~blank)
            if mask2.any():
                parsed2 = pd.to_datetime(raw[mask2], format="%d/%m/%Y", errors="coerce", utc=False)
                parsed.loc[mask2] = parsed2

            # Pass 3: explicit ISO YYYY-MM-DD for any still NaT
            mask3 = parsed.isna() & (~blank)
            if mask3.any():
                parsed3 = pd.to_datetime(raw[mask3], format="%Y-%m-%d", errors="coerce", utc=False)
                parsed.loc[mask3] = parsed3

            # mark failures after all passes
            type_fail[col] = (~blank) & parsed.isna()
            df_typed[col] = parsed
        elif pa.types.is_boolean(t):
            low = s.str.strip().str.lower().where(~blank, None)
            mapd = low.map(
                lambda v: True
                if v in {"true", "1", "yes", "y", "t"}
                else (False if v in {"false", "0", "no", "n", "f"} else pd.NA)
            )
            type_fail[col] = (~blank) & mapd.isna()
            df_typed[col] = mapd
        else:
            # strings: accept as-is
            df_typed[col] = s

        if not fld.nullable:
            null_fail[col] = blank

    bad_mask = type_fail.any(axis=1) | null_fail.any(axis=1)

    rejects = df_raw[bad_mask].copy()

    if not rejects.empty:
        def _reasons(idx):
            reasons = []
            for c in schema_cols:
                if type_fail.at[idx, c]:
                    reasons.append(f"invalid:{c}")
                if null_fail.at[idx, c]:
                    reasons.append(f"required_null:{c}")
            return ",".join(reasons)

        rejects["reject_reason"] = [_reasons(i) for i in rejects.index]

    clean_typed = df_typed[~bad_mask].copy()
    return clean_typed, rejects


# ----------------------- CSV loader (single or partitioned) -----------------------
def load_csv_generic(
    table_name: str,
    src: pathlib.Path,  # file OR directory
    schema: pa.Schema,
    lake_root: pathlib.Path,
    conn: duckdb.DuckDBPyConnection,
    partition_by: Optional[List[str]] = None,
    derive_from_path: Optional[List[str]] = None,
    derived_types: Optional[Dict[str, pa.DataType]] = None,
) -> int:
    derive_from_path = derive_from_path or []
    files: List[pathlib.Path] = []
    if src.is_file():
        files = [src]
    elif src.is_dir():
        part_files = sorted(src.rglob("part-*.csv"))
        files = part_files if part_files else sorted(src.glob("*.csv"))
    else:
        return 0

    total_rows = 0
    schema_cols = [f.name for f in schema]

    for csv_file in files:
        if already_processed(conn, csv_file):
            continue

        # 1) Read CSV with delimiter auto-detection and optional merge of unquoted commas
        bad_lines: list[str] = []

        # detect delimiter (comma, tab, semicolon, pipe)
        with open(csv_file, "r", encoding="utf-8", newline="") as fh:
            sample = fh.read(16384)
        import csv as _csv
        try:
            sniff = _csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
            _sep = sniff.delimiter
        except Exception:
            _sep = ","  # fallback

        # OPTIONAL: which column should swallow extra tokens (e.g. 'name')
        merge_unquoted_into = {"stores": "name", "suppliers": "name"}.get(table_name)

        # parse manually so we can fix only the bad rows (others pass through untouched)
        with open(csv_file, "r", encoding="utf-8", newline="") as f:
            reader = _csv.reader(f, delimiter=_sep, quotechar='"', escapechar="\\", skipinitialspace=True)
            header = next(reader)
            header = [h.strip() for h in header]
            exp_n = len(header)

            rows = []
            for tokens in reader:
                if len(tokens) == exp_n:
                    rows.append(tokens)
                    continue

                # too many columns → likely unquoted commas in a free-text field
                if len(tokens) > exp_n and merge_unquoted_into and merge_unquoted_into in header:
                    k = header.index(merge_unquoted_into)
                    extra = len(tokens) - exp_n
                    merged = tokens[:]
                    # merge the extra tokens directly to the right of the target column
                    merged[k] = ",".join([merged[k]] + merged[k+1:k+1+extra])
                    del merged[k+1:k+1+extra]
                    if len(merged) == exp_n:
                        rows.append(merged)
                        continue  # repaired successfully; not a reject
                # otherwise: record as malformed and skip
                bad_lines.append(_sep.join(tokens))

        # write parser-level rejects (only truly malformed lines)
        if bad_lines:
            rej_dir = lake_root / "_rejects" / table_name
            rej_dir.mkdir(parents=True, exist_ok=True)
            hint = csv_file.parent.name.replace("=", "_")
            pd.DataFrame({
                "reject_reason": ["csv_misaligned_columns"] * len(bad_lines),
                "src_filename": str(csv_file),
                "raw_line": bad_lines,
            }).to_csv(rej_dir / f"{csv_file.stem}_{hint}_badlines_rejects.csv", index=False)

        # build dataframe from the parsed rows
        df_raw = pd.DataFrame(rows, columns=header)

        # keep only schema columns, add missing as NA, and order them
        for c in schema_cols:
            if c not in df_raw.columns:
                df_raw[c] = pd.NA
        extra_cols = [c for c in df_raw.columns if c not in schema_cols]
        if extra_cols:
            df_raw.drop(columns=extra_cols, inplace=True)
        df_raw = df_raw[schema_cols]




        # 2) Validate to typed & row-level rejects (no repair)
        clean, row_rejects = _validate_to_typed_and_rejects(df_raw, schema)

        # 3) Write row-level rejects (if any)
        if not row_rejects.empty:
            rej_dir = lake_root / "_rejects" / table_name
            rej_dir.mkdir(parents=True, exist_ok=True)
            hint = csv_file.parent.name.replace("=", "_")
            row_rejects.assign(src_filename=str(csv_file)).to_csv(
                rej_dir / f"{csv_file.stem}_{hint}_rejects.csv", index=False
            )

        # 4) If no clean rows, mark processed and continue
        if clean.empty:
            mark_processed(conn, csv_file, 0)
            continue

        # 5) Attach derived partition columns (outside the canonical schema)
        parts = _parse_hive_parts(csv_file, derive_from_path) if derive_from_path else {}
        for c in derive_from_path:
            if c not in clean.columns:
                clean[c] = parts.get(c)

        # 6) Audit columns & row hash (hash over canonical schema columns only)
        clean["ingestion_ts"] = pd.Timestamp.now(tz="UTC")
        clean["src_filename"] = str(csv_file)
        clean["src_row_hash"] = clean[schema_cols].apply(lambda r: _hash_row_md5(r.values), axis=1)

        # 7) Build Arrow (without schema), then cast to final schema (+derived + audit)
        derived_not_in_schema = [c for c in derive_from_path if c not in schema_cols]
        schema_plus = pa.schema(
            list(schema)
            + _derived_field_defs(derived_not_in_schema, derived_types)
            + [
                pa.field("ingestion_ts", pa.timestamp("us")),
                pa.field("src_filename", pa.string()),
                pa.field("src_row_hash", pa.string()),
            ]
        )

        ordered_cols = schema_cols + derived_not_in_schema + ["ingestion_ts", "src_filename", "src_row_hash"]
        clean = clean[ordered_cols]
        clean_tbl = pa.Table.from_pandas(clean, preserve_index=False).cast(schema_plus, safe=False)

        # 8) Write Parquet & Delta
        pq_base = lake_root / "bronze" / "parquet" / table_name
        write_parquet_partitioned(clean_tbl, pq_base, partitioning=partition_by)

        dl_base = lake_root / "bronze" / "delta" / table_name
        write_delta(clean_tbl, dl_base, mode="append", partition_by=partition_by, schema_mode="merge")

        # 9) Manifest
        mark_processed(conn, csv_file, clean_tbl.num_rows)
        total_rows += int(clean_tbl.num_rows)

    return total_rows


# ----------------------- JSONL events (strict) -----------------------
def load_events(
    root: pathlib.Path,
    lake_root: pathlib.Path,
    conn: duckdb.DuckDBPyConnection,
    schema: pa.Schema,
) -> int:
    total = 0
    schema_cols = [f.name for f in schema]
    for p in sorted(root.glob("event_dt=*/events_*.jsonl")):
        if already_processed(conn, p):
            continue

        ok_lines, bad = [], []
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.rstrip("\n")
                try:
                    json.loads(s)
                    ok_lines.append(s)
                except Exception:
                    bad.append({"reject_reason": "malformed_json", "src_filename": str(p), "raw_line": s})

        if bad:
            rej_dir = lake_root / "_rejects" / "events"
            rej_dir.mkdir(parents=True, exist_ok=True)
            hint = p.parent.name.replace("=", "_")
            pd.DataFrame(bad).to_csv(rej_dir / f"{p.stem}_{hint}_rejects.csv", index=False)

        if not ok_lines:
            mark_processed(conn, p, 0)
            continue

        # event_dt from folder
        event_dt = p.parent.name.split("=", 1)[1] if "=" in p.parent.name else None

        # Build df_raw with only schema columns (usually ['json'])
        df_raw = pd.DataFrame({"json": ok_lines})
        clean, row_rejects = _validate_to_typed_and_rejects(df_raw, schema)

        if not row_rejects.empty:
            rej_dir = lake_root / "_rejects" / "events"
            rej_dir.mkdir(parents=True, exist_ok=True)
            hint = p.parent.name.replace("=", "_")
            row_rejects.assign(src_filename=str(p)).to_csv(
                rej_dir / f"{p.stem}_{hint}_rejects_cast.csv", index=False
            )

        if clean.empty:
            mark_processed(conn, p, 0)
            continue

        # Attach derived partition column (outside schema), audit, hash
        if "event_dt" not in clean.columns:
            clean["event_dt"] = event_dt

        clean["ingestion_ts"] = pd.Timestamp.now(tz="UTC")
        clean["src_filename"] = str(p)
        clean["src_row_hash"] = clean[schema_cols].apply(lambda r: _hash_row_md5(r.values), axis=1)

        derived_not_in_schema = ["event_dt"] if "event_dt" not in schema_cols else []
        schema_plus = pa.schema(
            list(schema)
            + _derived_field_defs(derived_not_in_schema, {"event_dt": pa.date32()})
            + [
                pa.field("ingestion_ts", pa.timestamp("us")),
                pa.field("src_filename", pa.string()),
                pa.field("src_row_hash", pa.string()),
            ]
        )
        ordered_cols = schema_cols + derived_not_in_schema + ["ingestion_ts", "src_filename", "src_row_hash"]
        clean = clean[ordered_cols]
        tbl = pa.Table.from_pandas(clean, preserve_index=False).cast(schema_plus, safe=False)

        pq_base = lake_root / "bronze" / "parquet" / "events"
        write_parquet_partitioned(tbl, pq_base, partitioning=["event_dt"])

        dl_base = lake_root / "bronze" / "delta" / "events"
        write_delta(tbl, dl_base, mode="append", partition_by=["event_dt"], schema_mode="merge")

        mark_processed(conn, p, tbl.num_rows)
        total += int(tbl.num_rows)
    return total


# ----------------------- Parquet (strict) -----------------------
def load_parquet_single(
    table_name: str,
    src: pathlib.Path,
    schema: pa.Schema,
    lake_root: pathlib.Path,
    conn: duckdb.DuckDBPyConnection,
) -> int:
    if not src.exists() or already_processed(conn, src):
        return 0
    tbl_raw = pq.read_table(src)
    df_raw = tbl_raw.to_pandas()
    clean, row_rejects = _validate_to_typed_and_rejects(df_raw, schema)

    if not row_rejects.empty:
        rej_dir = lake_root / "_rejects" / table_name
        rej_dir.mkdir(parents=True, exist_ok=True)
        row_rejects.assign(src_filename=str(src)).to_csv(rej_dir / f"{src.stem}_rejects.csv", index=False)

    if clean.empty:
        mark_processed(conn, src, 0)
        return 0

    schema_cols = [f.name for f in schema]
    clean["ingestion_ts"] = pd.Timestamp.now(tz="UTC")
    clean["src_filename"] = str(src)
    clean["src_row_hash"] = clean[schema_cols].apply(lambda r: _hash_row_md5(r.values), axis=1)

    schema_plus = pa.schema(
        list(schema)
        + [
            pa.field("ingestion_ts", pa.timestamp("us")),
            pa.field("src_filename", pa.string()),
            pa.field("src_row_hash", pa.string()),
        ]
    )
    ordered = schema_cols + ["ingestion_ts", "src_filename", "src_row_hash"]
    clean = clean[ordered]
    out_tbl = pa.Table.from_pandas(clean, preserve_index=False).cast(schema_plus, safe=False)

    pq_base = lake_root / "bronze" / "parquet" / table_name
    write_parquet_partitioned(out_tbl, pq_base, partitioning=None)

    dl_base = lake_root / "bronze" / "delta" / table_name
    write_delta(out_tbl, dl_base, mode="append", partition_by=None, schema_mode="merge")

    mark_processed(conn, src, out_tbl.num_rows)
    return int(out_tbl.num_rows)


# ----------------------- XLSX (strict) -----------------------
def load_xlsx_single(
    table_name: str,
    src: pathlib.Path,
    schema: pa.Schema,
    lake_root: pathlib.Path,
    conn: duckdb.DuckDBPyConnection,
    sheet: str = "rates",
) -> int:
    if not src.exists() or already_processed(conn, src):
        return 0
    df_raw = pd.read_excel(src, sheet_name=sheet, dtype=str).replace({"": None})
    clean, row_rejects = _validate_to_typed_and_rejects(df_raw, schema)

    if not row_rejects.empty:
        rej_dir = lake_root / "_rejects" / table_name
        rej_dir.mkdir(parents=True, exist_ok=True)
        row_rejects.assign(src_filename=str(src)).to_csv(rej_dir / f"{src.stem}_rejects.csv", index=False)

    if clean.empty:
        mark_processed(conn, src, 0)
        return 0

    schema_cols = [f.name for f in schema]
    clean["ingestion_ts"] = pd.Timestamp.now(tz="UTC")
    clean["src_filename"] = str(src)
    clean["src_row_hash"] = clean[schema_cols].apply(lambda r: _hash_row_md5(r.values), axis=1)

    schema_plus = pa.schema(
        list(schema)
        + [
            pa.field("ingestion_ts", pa.timestamp("us")),
            pa.field("src_filename", pa.string()),
            pa.field("src_row_hash", pa.string()),
        ]
    )
    ordered = schema_cols + ["ingestion_ts", "src_filename", "src_row_hash"]
    clean = clean[ordered]
    out_tbl = pa.Table.from_pandas(clean, preserve_index=False).cast(schema_plus, safe=False)

    pq_base = lake_root / "bronze" / "parquet" / table_name
    write_parquet_partitioned(out_tbl, pq_base, partitioning=None)

    dl_base = lake_root / "bronze" / "delta" / table_name
    write_delta(out_tbl, dl_base, mode="append", partition_by=None, schema_mode="merge")

    mark_processed(conn, src, out_tbl.num_rows)
    return int(out_tbl.num_rows)


# ----------------------- Main -----------------------
def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)

    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(args.manifest)
    init_manifest(conn)

    schemas = load_arrow_schemas()

    total = 0

    # ---- Dimensions (CSV, single files or simple dirs) ----
    total += load_csv_generic(
        table_name="customers",
        src=raw_root / "customers.csv",
        schema=schemas["customers"],
        lake_root=lake_root,
        conn=conn,
        partition_by=None,
        derive_from_path=None,
    )

    total += load_csv_generic(
        table_name="products",
        src=raw_root / "products.csv",
        schema=schemas["products"],
        lake_root=lake_root,
        conn=conn,
        partition_by=None,
        derive_from_path=None,
    )

    total += load_csv_generic(
        table_name="stores",
        src=raw_root / "stores.csv",
        schema=schemas["stores"],
        lake_root=lake_root,
        conn=conn,
        partition_by=None,
        derive_from_path=None,
    )

    total += load_csv_generic(
        table_name="suppliers",
        src=raw_root / "suppliers.csv",
        schema=schemas["suppliers"],
        lake_root=lake_root,
        conn=conn,
        partition_by=None,
        derive_from_path=None,
    )

    # ---- Facts (CSV, partitioned) ----
    # orders_header: partition by order_dt_local (present in file)
    total += load_csv_generic(
        table_name="orders_header",
        src=raw_root / "orders_header",
        schema=schemas["orders_header"],
        lake_root=lake_root,
        conn=conn,
        partition_by=["order_dt_local"],
        derive_from_path=None,  # not derived; column exists in CSV
    )

    # orders_lines: derive dt from path (dt=YYYY-MM-DD), partition by dt
    total += load_csv_generic(
        table_name="orders_lines",
        src=raw_root / "orders_lines",
        schema=schemas["orders_lines"],
        lake_root=lake_root,
        conn=conn,
        partition_by=["dt"],
        derive_from_path=["dt"],
        derived_types={"dt": pa.date32()},
    )

    # sensors: partition by store_id + month; derive month from path (add 'store_id' too if missing in file)
    total += load_csv_generic(
        table_name="sensors",
        src=raw_root / "sensors",
        schema=schemas["sensors"],
        lake_root=lake_root,
        conn=conn,
        partition_by=["store_id", "month"],
        derive_from_path=["month"],  # add "store_id" here too if it is not present in files
        derived_types={"month": pa.string()},
    )

    # ---- Events (JSONL, partitioned by event_dt) ----
    total += load_events(raw_root / "events", lake_root, conn, schema=schemas["events"])

    # ---- Exchange rates (XLSX) ----
    total += load_xlsx_single(
        table_name="exchange_rates",
        src=raw_root / "exchange_rates" / "exchange_rates.xlsx",
        schema=schemas["exchange_rates"],
        lake_root=lake_root,
        conn=conn,
        sheet="rates",
    )

    # ---- Shipments (Parquet) ----
    total += load_parquet_single(
        table_name="shipments",
        src=raw_root / "shipments.parquet",
        schema=schemas["shipments"],
        lake_root=lake_root,
        conn=conn,
    )

    # ---- Returns Day1 (Parquet preferred) ----
    ret_parq = raw_root / "returns_day1" / "part-00001.parquet"
    if ret_parq.exists():
        total += load_parquet_single(
            table_name="returns_day1",
            src=ret_parq,
            schema=schemas["returns_day1"],
            lake_root=lake_root,
            conn=conn,
        )

    print(f"✅ Bronze load complete. Total clean rows written: {total}")


if __name__ == "__main__":
    main()
