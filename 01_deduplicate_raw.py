"""Conservative full-source deduplication; no depth filling or ecological filtering.

Identity: exact parsed CSV values in every source column except id. Empty strings
are preserved. The earliest source record wins; every removed record is mapped.
Hash only routes records into partitions; equality is checked on all key columns.
Dependencies: duckdb. Run with --source PATH --out NEW_DIRECTORY.
"""
import argparse
import csv
import json
import time
from pathlib import Path

import duckdb

def qi(s):
    return '"' + s.replace('"', '""') + '"'


def qs(s):
    return "'" + str(s).replace("'", "''") + "'"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--partitions', type=int, default=64)
    args = ap.parse_args()
    src, out = Path(args.source), Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    start = time.time()
    with src.open(encoding='utf-8-sig', newline='') as f:
        cols = next(csv.reader(f))
    assert 'id' in cols and len(cols) == len(set(cols))
    keys = [c for c in cols if c != 'id']
    keysql = ','.join(map(qi, keys))
    colsql = ','.join(map(qi, cols))
    log = out / 'progress.jsonl'

    def emit(event, **kw):
        rec = dict(event=event, elapsed_seconds=round(time.time()-start, 2), **kw)
        print(json.dumps(rec, ensure_ascii=False), flush=True)
        with log.open('a', encoding='utf-8') as f:
            f.write(json.dumps(rec, ensure_ascii=False)+'\n')

    manifest = dict(source=str(src), bytes=src.stat().st_size,
                    source_mtime_ns=src.stat().st_mtime_ns,
                    rule='Exact parsed text equality on all columns except id; earliest source row retained',
                    columns=cols, keys=keys, partitions=args.partitions,
                    duckdb_version=duckdb.__version__, no_ecological_filtering=True)
    mp = out / 'manifest.json'
    if mp.exists():
        assert json.loads(mp.read_text(encoding='utf-8')) == manifest, 'Input/config changed'
    else:
        mp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    for d in ['staging', 'deduplicated', 'removed_mapping', 'checks', 'spill']:
        (out/d).mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    con.execute(f"SET temp_directory={qs(out/'spill')}")
    con.execute("SET max_temp_directory_size='65GB'")
    con.execute('SET preserve_insertion_order=true')
    con.execute('SET threads=1')
    con.execute('SET enable_progress_bar=false')
    stage_done = out / 'checks' / 'staging.json'
    if not stage_done.exists():
        if list((out/'staging').rglob('*.parquet')):
            raise RuntimeError('Incomplete staging exists; use a fresh output directory')
        emit('stage_start')
        schema = '{'+','.join(qs(c)+": 'VARCHAR'" for c in cols)+'}'
        relation = (f"read_csv({qs(src)}, header=true, columns={schema}, "
                    "nullstr='__CODEX_NO_NULL_SENTINEL__', strict_mode=true, parallel=false)")
        con.execute(f"COPY (SELECT row_number() OVER ()::BIGINT AS source_row, *, "
                    f"(hash({keysql}) % {args.partitions})::INTEGER AS bucket FROM {relation}) "
                    f"TO {qs(out/'staging')} (FORMAT PARQUET, COMPRESSION ZSTD, "
                    "PARTITION_BY(bucket), ROW_GROUP_SIZE 100000)")
        n = con.execute(f"SELECT count(*) FROM read_parquet({qs(out/'staging'/'*'/'*.parquet')})").fetchone()[0]
        stage_done.write_text(json.dumps(dict(raw_rows=n)), encoding='utf-8')
        emit('stage_complete', raw_rows=n)
    raw_n = json.loads(stage_done.read_text())['raw_rows']
    # Source row identifiers are already materialized. Parallel partition processing
    # is now deterministic because the retained row is an explicit MIN(source_row).
    con.execute('SET threads=6')
    con.execute("SET memory_limit='8GB'")
    con.execute('SET preserve_insertion_order=false')
    emit('dedup_configuration', threads=6, memory_limit='8GB')
    summaries = []
    for b in range(args.partitions):
        check = out/'checks'/f'part_{b:03}.json'
        if check.exists():
            summaries.append(json.loads(check.read_text()))
            continue
        emit('partition_start', bucket=b)
        part = out/'staging'/f'bucket={b}'/'*.parquet'
        keep = out/'deduplicated'/f'part_{b:03}.parquet'
        removed = out/'removed_mapping'/f'part_{b:03}.parquet'
        con.execute(f"CREATE OR REPLACE TEMP TABLE ranked AS SELECT {colsql}, source_row, "
                    f"min(source_row) OVER w AS retained_source_row, count(*) OVER w AS duplicate_group_size "
                    f"FROM read_parquet({qs(part)}) WINDOW w AS (PARTITION BY {keysql})")
        counts = con.execute('SELECT count(*), count(*) FILTER (WHERE source_row=retained_source_row), '
                             'count(*) FILTER (WHERE source_row=retained_source_row AND duplicate_group_size>1), '
                             'max(duplicate_group_size) FROM ranked').fetchone()
        con.execute(f"COPY (SELECT {colsql}, source_row, duplicate_group_size FROM ranked "
                    f"WHERE source_row=retained_source_row) TO {qs(keep)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.execute(f"COPY (SELECT r.source_row AS removed_source_row, r.id AS removed_id, "
                    "r.retained_source_row, k.id AS retained_id, (r.id=k.id) AS same_id "
                    "FROM ranked r JOIN ranked k ON r.retained_source_row=k.source_row "
                    f"WHERE r.source_row<>r.retained_source_row) TO {qs(removed)} "
                    "(FORMAT PARQUET, COMPRESSION ZSTD)")
        kn = con.execute(f'SELECT count(*) FROM read_parquet({qs(keep)})').fetchone()[0]
        rn, same = con.execute(f'SELECT count(*), count(*) FILTER (WHERE same_id) FROM read_parquet({qs(removed)})').fetchone()
        assert kn == counts[1] and kn+rn == counts[0]
        rec = dict(bucket=b, input_rows=counts[0], retained_rows=kn, removed_rows=rn,
                   repeated_groups=counts[2], max_group_size=counts[3], removed_same_id=same)
        check.write_text(json.dumps(rec), encoding='utf-8')
        summaries.append(rec)
        con.execute('DROP TABLE ranked')
        emit('partition_complete', **rec)
    summary = dict(raw_rows=raw_n, retained_rows=sum(x['retained_rows'] for x in summaries),
                   removed_rows=sum(x['removed_rows'] for x in summaries),
                   repeated_groups=sum(x['repeated_groups'] for x in summaries),
                   removed_same_id=sum(x['removed_same_id'] for x in summaries),
                   max_group_size=max(x['max_group_size'] for x in summaries))
    assert summary['raw_rows'] == summary['retained_rows']+summary['removed_rows']
    assert summary['raw_rows'] == sum(x['input_rows'] for x in summaries)
    assert src.stat().st_size == manifest['bytes'] and src.stat().st_mtime_ns == manifest['source_mtime_ns']
    summary['removed_percent'] = 100*summary['removed_rows']/raw_n
    summary['removed_different_id'] = summary['removed_rows']-summary['removed_same_id']
    summary['completed'] = True
    (out/'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    emit('completed', **summary)
    con.close()


if __name__ == '__main__':
    main()
