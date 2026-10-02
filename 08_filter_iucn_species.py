"""Intersect coordinate-qualified Dou records with ALL supplied IUCN names.

No depth, year, environment, abundance or range-polygon filtering/filling.
Only trim scientific names and lowercase for matching; preserve original fields.
"""
import argparse
import hashlib
import json
from pathlib import Path
import duckdb
import pandas as pd

def q(s):
    return "'"+str(s).replace("'", "''")+"'"

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--source',required=True)
    ap.add_argument('--iucn',required=True)
    ap.add_argument('--out',required=True)
    a=ap.parse_args()
    src,iucn,out=Path(a.source),Path(a.iucn),Path(a.out)
    out.mkdir(parents=True,exist_ok=False)
    for name in ['accepted','excluded_index','checks']:
        (out/name).mkdir()
    frame=pd.read_excel(iucn)
    names=frame['sci_name'].dropna().astype(str).str.strip().str.lower()
    names=sorted(set(names[names!='']))
    lookup=pd.DataFrame({'species_key':names})
    lookup.to_csv(out/'iucn_species_names.csv',index=False,encoding='utf-8-sig')
    manifest=dict(source=str(src),iucn_file=str(iucn),iucn_sha256=hashlib.sha256(iucn.read_bytes()).hexdigest(),iucn_rows=len(frame),iucn_unique_names=len(names),matching='lower(trim(scientificname)); exact membership; no synonym resolution',depth_processed=False)
    (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    con=duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute('SET threads=4')
    con.register('names_df',lookup)
    con.execute('CREATE TEMP TABLE names AS SELECT * FROM names_df')
    parts=sorted(src.glob('part_*.parquet'))
    assert len(parts)==64
    total=kept=0
    for part in parts:
        con.execute(f"CREATE OR REPLACE TEMP TABLE batch AS SELECT *,lower(trim(scientificname)) IN (SELECT species_key FROM names) AS matched_iucn FROM read_parquet({q(part)})")
        n,k=con.execute('SELECT count(*),count(*) FILTER(WHERE matched_iucn) FROM batch').fetchone()
        con.execute(f"COPY (SELECT * EXCLUDE(matched_iucn) FROM batch WHERE matched_iucn) TO {q(out/'accepted'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        con.execute(f"COPY (SELECT source_row,id,scientificname,'not_in_iucn_name_list' AS exclusion_reason FROM batch WHERE NOT coalesce(matched_iucn,false)) TO {q(out/'excluded_index'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
        actual=con.execute(f"SELECT count(*) FROM read_parquet({q(out/'accepted'/part.name)})").fetchone()[0]
        rejected=con.execute(f"SELECT count(*) FROM read_parquet({q(out/'excluded_index'/part.name)})").fetchone()[0]
        assert actual==k and actual+rejected==n
        total+=n;kept+=k
        stat=dict(partition=part.name,input_rows=n,accepted_rows=k,excluded_rows=n-k)
        (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(stat),encoding='utf-8')
        print(json.dumps(stat),flush=True)
    con.execute(f"CREATE VIEW accepted AS SELECT * FROM read_parquet({q(out/'accepted'/'*.parquet')})")
    con.execute(f"COPY (SELECT lower(trim(scientificname)) AS species_key,count(*) AS records FROM accepted GROUP BY 1 ORDER BY 1) TO {q(out/'retained_species_counts.csv')} (HEADER)")
    species=con.execute('SELECT count(distinct lower(trim(scientificname))) FROM accepted').fetchone()[0]
    unmatched=con.execute('SELECT count(*) FROM accepted WHERE lower(trim(scientificname)) NOT IN (SELECT species_key FROM names) OR scientificname IS NULL').fetchone()[0]
    assert unmatched==0
    con.execute(f"COPY (SELECT species_key FROM names WHERE species_key NOT IN (SELECT lower(trim(scientificname)) FROM accepted) ORDER BY 1) TO {q(out/'iucn_names_without_retained_records.csv')} (HEADER)")
    summary=dict(**manifest,input_rows=total,accepted_rows=kept,excluded_rows=total-kept,retained_species=species,iucn_names_without_retained_records=len(names)-species,all_partition_counts_verified=True,all_retained_names_matched=True,completed=True)
    (out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=True),flush=True)

if __name__=='__main__':
    main()
