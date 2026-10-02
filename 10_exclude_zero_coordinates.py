"""Exclude exactly (longitude,latitude)=(0,0); preserve all original fields.

This is an explicitly approved project QC rule, not a universal geographic rule.
No filtering of single-axis zeros, depth, year, environment or record counts.
"""
import argparse,json,hashlib
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--out',required=True);a=p.parse_args()
 src,out=Path(a.source),Path(a.out);out.mkdir(parents=True,exist_ok=False)
 for name in ['accepted','excluded','checks']:(out/name).mkdir()
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 z='try_cast(longitude AS DOUBLE)=0 AND try_cast(latitude AS DOUBLE)=0'
 total=removed=0;files=[]
 for part in sorted(src.glob('part_*.parquet')):
  c.execute(f'CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM read_parquet({q(part)})')
  n,r=c.execute(f'SELECT count(*),count(*) FILTER(WHERE {z}) FROM batch').fetchone()
  c.execute(f"COPY (SELECT * FROM batch WHERE NOT ({z})) TO {q(out/'accepted'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
  c.execute(f"COPY (SELECT *,'zero_zero_coordinate' AS exclusion_reason FROM batch WHERE {z}) TO {q(out/'excluded'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
  actual=c.execute(f'SELECT count(*) FROM read_parquet({q(out/"accepted"/part.name)})').fetchone()[0]
  rejected=c.execute(f'SELECT count(*) FROM read_parquet({q(out/"excluded"/part.name)})').fetchone()[0]
  assert actual+rejected==n and rejected==r
  check=dict(partition=part.name,input_rows=n,accepted_rows=actual,excluded_rows=r)
  (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(check),encoding='utf-8')
  files.append(dict(partition=part.name,input_sha256=sha(part),accepted_sha256=sha(out/'accepted'/part.name),excluded_sha256=sha(out/'excluded'/part.name)))
  total+=n;removed+=r;print(json.dumps(check),flush=True)
 c.execute(f'CREATE VIEW accepted AS SELECT * FROM read_parquet({q(out/"accepted"/"*.parquet")})')
 species,zero=c.execute(f'SELECT count(distinct lower(trim(scientificname))),count(*) FILTER(WHERE {z}) FROM accepted').fetchone();assert zero==0
 c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key,count(*) records FROM accepted GROUP BY 1 ORDER BY 1) TO {q(out/'retained_species_counts.csv')} (HEADER)")
 c.execute(f"COPY (SELECT * FROM read_parquet({q(out/'excluded'/'*.parquet')})) TO {q(out/'excluded_zero_coordinates.csv')} (HEADER)")
 s=dict(source=str(src),rule='Exclude both coordinates zero only; retain single-axis zero',input_rows=total,accepted_rows=total-removed,excluded_rows=removed,retained_species=species,depth_year_environment_unchanged=True,all_partition_counts_verified=True,completed=True)
 (out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8')
 (out/'file_checksums.json').write_text(json.dumps(files,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=True),flush=True)
if __name__=='__main__':main()
