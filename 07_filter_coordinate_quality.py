"""Basic coordinate QC only. Preserve source values; never modify source files.

Exclude missing, unparseable/nonfinite, or out-of-range coordinates.
Retain zero coordinates and flag (0,0) for review. No spatial mask or depth rules.
"""
import argparse
import json
from pathlib import Path
import duckdb

def q(s):
    return "'" + str(s).replace("'", "''") + "'"

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', required=True)
    p.add_argument('--out', required=True)
    a = p.parse_args()
    src, out = Path(a.source), Path(a.out)
    out.mkdir(parents=True, exist_ok=False)
    for name in ['accepted', 'excluded', 'checks']:
        (out / name).mkdir()
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    con.execute('SET threads=4')
    parts = sorted(src.glob('part_*.parquet'))
    assert len(parts) == 64, 'Expected complete 64-part deduplicated source'
    totals = {}
    statuses = []
    for axis, limit in [('longitude', 180), ('latitude', 90)]:
        statuses.append(f"CASE WHEN {axis} IS NULL OR lower(trim({axis})) IN ('','na','n/a','nan','null','none') THEN 'missing' WHEN try_cast({axis} AS DOUBLE) IS NULL OR NOT isfinite(try_cast({axis} AS DOUBLE)) THEN 'invalid_numeric' WHEN abs(try_cast({axis} AS DOUBLE))>{limit} THEN 'out_of_range' ELSE 'valid' END AS {axis}_qc")
    for part in parts:
        con.execute(f"CREATE OR REPLACE TEMP TABLE qc AS SELECT *, try_cast(longitude AS DOUBLE) AS longitude_numeric, try_cast(latitude AS DOUBLE) AS latitude_numeric, {','.join(statuses)} FROM read_parquet({q(part)})")
        valid = "longitude_qc='valid' AND latitude_qc='valid'"
        groups = con.execute('SELECT longitude_qc,latitude_qc,count(*) FROM qc GROUP BY 1,2').fetchall()
        for lon, lat, n in groups:
            key = lon + ' / ' + lat
            totals[key] = totals.get(key, 0) + n
        con.execute(f"COPY (SELECT *, (longitude_numeric=0 AND latitude_numeric=0) AS review_zero_zero FROM qc WHERE {valid}) TO {q(out/'accepted'/part.name)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        con.execute(f"COPY (SELECT * FROM qc WHERE NOT ({valid})) TO {q(out/'excluded'/part.name)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(groups), encoding='utf-8')
        print(json.dumps({'part':part.name,'completed':len(list((out/'checks').glob('*.json'))),'groups':groups}),flush=True)
    accepted = q(out/'accepted'/'*.parquet')
    excluded = q(out/'excluded'/'*.parquet')
    n, invalid, zero_lon, zero_lat, zero_both = con.execute(f"SELECT count(*),count(*) FILTER(WHERE NOT ({valid})),count(*) FILTER(WHERE longitude_numeric=0),count(*) FILTER(WHERE latitude_numeric=0),count(*) FILTER(WHERE review_zero_zero) FROM read_parquet({accepted})").fetchone()
    rejected = con.execute(f'SELECT count(*) FROM read_parquet({excluded})').fetchone()[0]
    assert invalid == 0 and n+rejected == sum(totals.values())
    assert n == totals.get('valid / valid', 0)
    summary = dict(source=str(src),input_rows=n+rejected,accepted_rows=n,excluded_rows=rejected,coordinate_status_counts=totals,retained_zero_longitude=zero_lon,retained_zero_latitude=zero_lat,retained_zero_zero=zero_both,missing_tokens=['','na','n/a','nan','null','none'],bounds={'longitude':[-180,180],'latitude':[-90,90]},raw_fields_preserved=True,depth_environment_year_unchanged=True,land_mask_applied=False,completed=True)
    (out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False),flush=True)

if __name__ == '__main__':
    main()
