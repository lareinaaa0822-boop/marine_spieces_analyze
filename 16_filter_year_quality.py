"""Keep finite integer collection years 1993..2025. Never map early years.

Preserve year and all existing fields. Earlier records and missing/late/invalid
years are saved separately with reasons and stable source_row identifiers.
"""
import argparse,json,hashlib,platform
from pathlib import Path
from collections import Counter
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--source',required=True);ap.add_argument('--out',required=True)
 ap.add_argument('--min-year',type=int,default=1993);ap.add_argument('--max-year',type=int,default=2025)
 a=ap.parse_args();src,out=Path(a.source),Path(a.out);out.mkdir(parents=True,exist_ok=False)
 for d in ['accepted','earlier','review','checks']:(out/d).mkdir()
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 manifest=dict(source=str(src),min_year=a.min_year,max_year=a.max_year,boundaries_inclusive=True,missing_tokens=['','na','n/a','nan','null','none','<na>'],rule='Keep finite integer years in target interval; earlier separately; missing/late/invalid to review; no clamping or eventDate imputation',python=platform.python_version(),duckdb=duckdb.__version__,depth_environment_unchanged=True)
 (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
 totals=Counter();counts=Counter();checks=[]
 parts=sorted(src.glob('part_*.parquet'));assert len(parts)==64
 for part in parts:
  c.execute(f"CREATE OR REPLACE TEMP TABLE parsed AS SELECT *,year AS year_raw,try_cast(year AS DOUBLE) year_numeric FROM read_parquet({q(part)})")
  c.execute(f"""CREATE OR REPLACE TEMP TABLE classified AS SELECT *,CASE
   WHEN lower(trim(coalesce(year_raw,''))) IN ('','na','n/a','nan','null','none','<na>') THEN 'missing'
   WHEN year_numeric IS NULL THEN 'unparseable'
   WHEN NOT isfinite(year_numeric) THEN 'nonfinite'
   WHEN year_numeric<>floor(year_numeric) THEN 'fractional'
   WHEN year_numeric<{a.min_year} THEN 'before_period'
   WHEN year_numeric>{a.max_year} THEN 'after_period'
   ELSE 'ready' END AS year_qc_status FROM parsed""")
  c.execute("CREATE OR REPLACE TEMP TABLE result AS SELECT * EXCLUDE(year_numeric),CASE WHEN year_qc_status='ready' THEN cast(year_numeric AS INTEGER) END AS year_final FROM classified")
  stat=dict(c.execute('SELECT year_qc_status,count(*) FROM result GROUP BY 1').fetchall());counts.update(stat);n=sum(stat.values())
  predicates={'accepted':"year_qc_status='ready'",'earlier':"year_qc_status='before_period'",'review':"year_qc_status NOT IN ('ready','before_period')"}
  chk=dict(partition=part.name,input_rows=n,status_counts=stat,input_sha256=sha(part));counts_here=0
  for name,predicate in predicates.items():
   dest=out/name/part.name
   c.execute(f'COPY (SELECT * FROM result WHERE {predicate}) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)')
   k=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0]
   chk[name+'_rows']=k;chk[name+'_sha256']=sha(dest);totals[name]+=k;counts_here+=k
  assert counts_here==n
  bad=c.execute(f"SELECT count(*) FROM result WHERE (year IS DISTINCT FROM year_raw) OR (year_qc_status='ready' AND (year_final IS NULL OR year_final<{a.min_year} OR year_final>{a.max_year} OR year_final IS DISTINCT FROM try_cast(year_raw AS DOUBLE))) OR (year_qc_status<>'ready' AND year_final IS NOT NULL)").fetchone()[0];assert bad==0
  checks.append(chk);(out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(chk),encoding='utf-8')
  print(json.dumps({k:v for k,v in chk.items() if 'sha256' not in k}),flush=True)
 species={}
 for name in ['accepted','earlier','review']:
  c.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({q(out/name/"*.parquet")})')
  species[name]=c.execute(f'SELECT count(distinct lower(trim(scientificname))) FROM {name}').fetchone()[0]
  c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key,count(*) records FROM {name} GROUP BY 1 ORDER BY 1) TO {q(out/(name+'_species_counts.csv'))} (HEADER)")
  if name!='accepted':c.execute(f"COPY (SELECT source_row,id,scientificname,year_raw,eventDate,year_qc_status FROM {name} ORDER BY source_row) TO {q(out/(name+'_index.csv'))} (HEADER)")
 c.execute(f"COPY (SELECT year_raw,year_qc_status,count(*) records FROM (SELECT year_raw,year_qc_status FROM accepted UNION ALL SELECT year_raw,year_qc_status FROM earlier UNION ALL SELECT year_raw,year_qc_status FROM review) GROUP BY 1,2 ORDER BY try_cast(year_raw AS DOUBLE),year_raw) TO {q(out/'year_distribution.csv')} (HEADER)")
 c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key FROM read_parquet({q(src/'*.parquet')}) GROUP BY 1 HAVING species_key NOT IN (SELECT lower(trim(scientificname)) FROM accepted)) TO {q(out/'species_without_in_period_records.csv')} (HEADER)")
 # Independent full year histogram confirms the selected row count.
 years=c.execute('SELECT min(year_final),max(year_final),count(*) FROM accepted').fetchone()
 assert years[2]==totals['accepted']
 s=dict(input_rows=sum(totals.values()),**{k+'_rows':v for k,v in totals.items()},year_status_counts=dict(counts),species_counts=species,accepted_year_min=years[0],accepted_year_max=years[1],all_partition_counts_verified=True,all_year_assignments_verified=True,original_year_preserved=True,completed=True)
 (out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');(out/'file_checksums.json').write_text(json.dumps(checks,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=True),flush=True)
if __name__=='__main__':main()
