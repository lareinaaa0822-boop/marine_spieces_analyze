"""Depth stage: observed priority, missing-only IUCN range-midpoint fill.

Never absolute-value negative depths or use negative/unknown IUCN endpoints.
Invalid observed depths go to review, not imputation. Preserve original fields.
Species with invalid IUCN ranges can retain valid observed depths.
"""
import argparse,json,hashlib,math,platform
from pathlib import Path
from collections import Counter
import pandas as pd
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 ap=argparse.ArgumentParser()
 for k in ['source','iucn','out']:ap.add_argument('--'+k,required=True)
 ap.add_argument('--max-depth',type=float,default=11000)
 ap.add_argument('--environment-max-depth',type=float,default=5727.9)
 a=ap.parse_args();src,iucn,out=Path(a.source),Path(a.iucn),Path(a.out)
 out.mkdir(parents=True,exist_ok=False)
 for d in ['accepted','review','excluded','checks']:(out/d).mkdir()
 frame=pd.read_excel(iucn);frame['species_key']=frame.sci_name.astype('string').str.strip().str.lower()
 ranges=[]
 for name,g in frame.dropna(subset=['species_key']).groupby('species_key',sort=True):
  pairs=[]
  for up,low in zip(g.depth_up,g.depth_low):
   vals=[]
   for x in [up,low]:
    v=pd.to_numeric(x,errors='coerce');vals.append(float(v) if pd.notna(v) and math.isfinite(float(v)) else None)
   pairs.append(tuple(vals))
  distinct=set(pairs);up,low=pairs[0]
  if len(distinct)>1:status='conflicting'
  elif up is None or low is None:status='missing_endpoint'
  elif up<0 or low<0:status='negative_endpoint'
  elif up>low:status='reversed'
  elif low>a.max_depth:status='excessive_endpoint'
  else:status='valid'
  ranges.append(dict(species_key=name,iucn_depth_up=up,iucn_depth_low=low,iucn_depth_mid=(up+low)/2 if status=='valid' else None,iucn_range_status=status,iucn_table_rows=len(g)))
 lookup=pd.DataFrame(ranges);lookup.to_csv(out/'iucn_depth_lookup.csv',index=False,encoding='utf-8-sig')
 lookup[lookup.iucn_range_status!='valid'].to_csv(out/'iucn_unusable_ranges.csv',index=False,encoding='utf-8-sig')
 manifest=dict(source=str(src),iucn_file=str(iucn),iucn_sha256=sha(iucn),max_valid_observed_depth=a.max_depth,environment_max_depth_flag=a.environment_max_depth,python=platform.python_version(),duckdb=duckdb.__version__,pandas=pd.__version__,rules={'observed':'finite numeric 0..max_depth inclusive; priority regardless of IUCN range availability','imputation':'only genuinely missing observed depth; IUCN finite nonnegative ordered endpoints and no conflicting pairs; (up+low)/2','review':'negative, excessive, nonfinite or unparseable observed depth; missing observed with reversed/conflicting IUCN range','excluded':'missing observed and no usable IUCN endpoints','depth_flag':'observed=0,iucn_mid=1,unresolved=NULL','missing_tokens':['','na','n/a','nan','null','none','<na>']},no_year_environment_or_spatial_sampling=True)
 (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4');c.register('lookup_df',lookup)
 c.execute('CREATE TEMP TABLE lookup AS SELECT * FROM lookup_df')
 parts=sorted(src.glob('part_*.parquet'));assert len(parts)==64
 counts=Counter();range_counts=Counter();total=accepted=review=excluded=0;files=[]
 for part in parts:
  c.execute(f"CREATE OR REPLACE TEMP TABLE input_depth AS SELECT r.*,r.depth AS depth_raw,try_cast(r.depth AS DOUBLE) AS depth_numeric,l.* EXCLUDE(species_key) FROM read_parquet({q(part)}) r LEFT JOIN lookup l ON lower(trim(r.scientificname))=l.species_key")
  c.execute(f"""CREATE OR REPLACE TEMP TABLE classified AS SELECT *,CASE
   WHEN lower(trim(coalesce(depth_raw,''))) IN ('','na','n/a','nan','null','none','<na>') THEN 'missing'
   WHEN depth_numeric IS NULL THEN 'unparseable'
   WHEN NOT isfinite(depth_numeric) THEN 'nonfinite'
   WHEN depth_numeric<0 THEN 'negative'
   WHEN depth_numeric>{a.max_depth} THEN 'excessive'
   ELSE 'valid' END AS observed_depth_status FROM input_depth""")
  c.execute("""CREATE OR REPLACE TEMP TABLE qc AS SELECT *,CASE
   WHEN observed_depth_status='valid' THEN 'ready_observed'
   WHEN observed_depth_status<>'missing' THEN 'review_'||observed_depth_status||'_observed'
   WHEN iucn_range_status='valid' THEN 'ready_iucn_mid'
   WHEN iucn_range_status IN ('reversed','conflicting','excessive_endpoint') THEN 'review_iucn_'||iucn_range_status
   ELSE 'excluded_missing_depth_'||coalesce(iucn_range_status,'unmatched_iucn') END AS depth_qc_status,
   CASE WHEN observed_depth_status='valid' THEN depth_numeric END AS depth_observed,
   CASE WHEN observed_depth_status='valid' THEN depth_numeric WHEN observed_depth_status='missing' AND iucn_range_status='valid' THEN iucn_depth_mid END AS depth_final,
   CASE WHEN observed_depth_status='valid' THEN 'observed' WHEN observed_depth_status='missing' AND iucn_range_status='valid' THEN 'iucn_mid' ELSE 'unresolved' END AS depth_source,
   CASE WHEN observed_depth_status='valid' THEN 0 WHEN observed_depth_status='missing' AND iucn_range_status='valid' THEN 1 END AS depth_flag FROM classified""")
  c.execute(f"CREATE OR REPLACE TEMP TABLE result AS SELECT * EXCLUDE(depth_numeric),coalesce(depth_final>{a.environment_max_depth},false) AS beyond_environment_depth,coalesce(observed_depth_status='valid' AND iucn_range_status='valid' AND (depth_numeric<iucn_depth_up OR depth_numeric>iucn_depth_low),false) AS observed_outside_iucn_range FROM qc")
  stat=c.execute('SELECT depth_qc_status,count(*) FROM result GROUP BY 1').fetchall();counts.update(dict(stat))
  range_counts.update(dict(c.execute('SELECT coalesce(iucn_range_status,\'unmatched\'),count(*) FROM result GROUP BY 1').fetchall()))
  n=sum(x[1] for x in stat);ka=kr=ke=0
  for name,prefix in [('accepted','ready_'),('review','review_'),('excluded','excluded_')]:
   dest=out/name/part.name
   c.execute(f"COPY (SELECT * FROM result WHERE starts_with(depth_qc_status,{q(prefix)})) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)")
   k=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0]
   if name=='accepted':ka=k
   elif name=='review':kr=k
   else:ke=k
  assert ka+kr+ke==n
  failures=c.execute(f"SELECT count(*) FROM result WHERE (depth_source='observed' AND depth_final IS DISTINCT FROM depth_observed) OR (depth_source='iucn_mid' AND (observed_depth_status<>'missing' OR iucn_range_status<>'valid' OR depth_final IS DISTINCT FROM (iucn_depth_up+iucn_depth_low)/2)) OR (starts_with(depth_qc_status,'ready_') AND (depth_final IS NULL OR NOT isfinite(depth_final) OR depth_final<0 OR depth_final>{a.max_depth}))").fetchone()[0]
  assert failures==0
  chk=dict(partition=part.name,input_rows=n,accepted_rows=ka,review_rows=kr,excluded_rows=ke,status_counts=dict(stat),input_sha256=sha(part))
  for name in ['accepted','review','excluded']:chk[name+'_sha256']=sha(out/name/part.name)
  (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(chk),encoding='utf-8');files.append(chk)
  total+=n;accepted+=ka;review+=kr;excluded+=ke;print(json.dumps({k:v for k,v in chk.items() if 'sha256' not in k}),flush=True)
 assert accepted+review+excluded==total
 tables={}
 for name in ['accepted','review','excluded']:
  c.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({q(out/name/"*.parquet")})')
  c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key,count(*) records FROM {name} GROUP BY 1 ORDER BY 1) TO {q(out/(name+'_species_counts.csv'))} (HEADER)")
  tables[name]=c.execute(f'SELECT count(distinct lower(trim(scientificname))) FROM {name}').fetchone()[0]
  if name!='accepted':c.execute(f"COPY (SELECT source_row,id,scientificname,depth_raw,observed_depth_status,iucn_depth_up,iucn_depth_low,iucn_range_status,depth_qc_status FROM {name} ORDER BY source_row) TO {q(out/(name+'_index.csv'))} (HEADER)")
 flags=dict(zip(['beyond_environment_depth_rows','observed_outside_iucn_range_rows','observed_with_unusable_iucn_range_rows'],c.execute("SELECT count(*) FILTER(WHERE beyond_environment_depth),count(*) FILTER(WHERE observed_outside_iucn_range),count(*) FILTER(WHERE depth_source='observed' AND iucn_range_status<>'valid') FROM accepted").fetchone()))
 c.execute(f"COPY (SELECT source_row,id,scientificname,depth_raw,depth_final,depth_source,iucn_depth_up,iucn_depth_low,iucn_range_status,beyond_environment_depth,observed_outside_iucn_range FROM accepted WHERE beyond_environment_depth OR observed_outside_iucn_range) TO {q(out/'accepted_depth_flags.csv')} (HEADER)")
 c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key FROM read_parquet({q(src/'*.parquet')}) GROUP BY 1 HAVING species_key NOT IN (SELECT lower(trim(scientificname)) FROM accepted)) TO {q(out/'species_without_ready_depth_records.csv')} (HEADER)")
 s=dict(input_rows=total,accepted_rows=accepted,review_rows=review,excluded_rows=excluded,species_counts=tables,depth_status_counts=dict(counts),record_iucn_range_status_counts=dict(range_counts),iucn_lookup_species_status_counts=lookup.iucn_range_status.value_counts().to_dict(),**flags,all_partition_counts_verified=True,all_depth_assignments_verified=True,completed=True)
 (out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');(out/'file_checksums.json').write_text(json.dumps(files,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=True),flush=True)
if __name__=='__main__':main()
