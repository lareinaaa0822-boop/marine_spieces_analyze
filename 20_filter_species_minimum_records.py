"""Species-level preliminary filter after year QC; preserve every input field."""
import argparse,json,hashlib,platform
from pathlib import Path
import duckdb

def q(p):return "'"+str(p).replace("'","''")+"'"
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--source',required=True);ap.add_argument('--out',required=True);ap.add_argument('--min-records',type=int,default=15);ap.add_argument('--verify-existing',action='store_true');a=ap.parse_args()
 assert a.min_records>0
 src,out=Path(a.source),Path(a.out);parts=sorted(src.glob('part_*.parquet'));assert len(parts)==64
 out.mkdir(exist_ok=a.verify_existing,parents=True)
 for d in ['accepted','below_threshold','checks']:(out/d).mkdir(exist_ok=a.verify_existing)
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 c.execute(f'CREATE VIEW source AS SELECT * FROM read_parquet({q(src/"part_*.parquet")})')
 assert c.execute("SELECT count(*) FROM source WHERE scientificname IS NULL OR trim(scientificname)='' ").fetchone()[0]==0
 c.execute('CREATE TEMP TABLE species_counts AS SELECT lower(trim(scientificname)) species_key,min(scientificname) scientificname,count(*) records FROM source GROUP BY 1')
 c.execute(f"COPY (SELECT *, records>={a.min_records} retained FROM species_counts ORDER BY species_key) TO {q(out/'all_species_counts.csv')} (HEADER)")
 for name,pred in [('accepted',f'records>={a.min_records}'),('below_threshold',f'records<{a.min_records}')]:
  c.execute(f'COPY (SELECT * FROM species_counts WHERE {pred} ORDER BY species_key) TO {q(out/(name+"_species_counts.csv"))} (HEADER)')
 c.execute(f"COPY (SELECT records,count(*) species FROM species_counts GROUP BY 1 ORDER BY 1) TO {q(out/'record_count_distribution.csv')} (HEADER)")
 checks=[];totals={'accepted':0,'below_threshold':0}
 for part in parts:
  c.execute(f'CREATE OR REPLACE TEMP VIEW part AS SELECT * FROM read_parquet({q(part)})')
  n=c.execute('SELECT count(*) FROM part').fetchone()[0];chk=dict(part=part.name,input_rows=n,input_sha256=sha(part))
  for name,pred in [('accepted',f'records>={a.min_records}'),('below_threshold',f'records<{a.min_records}')]:
   dest=out/name/part.name
   if not a.verify_existing:c.execute(f'COPY (SELECT p.* FROM part p JOIN species_counts s ON lower(trim(p.scientificname))=s.species_key WHERE s.{pred}) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)')
   count=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0];chk[name+'_rows']=count;chk[name+'_sha256']=sha(dest);totals[name]+=count
  assert chk['accepted_rows']+chk['below_threshold_rows']==n
  union_part=f'SELECT * FROM read_parquet({q(out/"accepted"/part.name)}) UNION ALL SELECT * FROM read_parquet({q(out/"below_threshold"/part.name)})'
  assert c.execute(f'SELECT count(*) FROM (SELECT * FROM part EXCEPT ALL ({union_part}))').fetchone()[0]==0
  assert c.execute(f'SELECT count(*) FROM (({union_part}) EXCEPT ALL SELECT * FROM part)').fetchone()[0]==0
  chk['all_fields_preserved']=True;checks.append(chk)
  (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(chk,indent=2),encoding='utf-8')
  print(part.name,chk['accepted_rows'],chk['below_threshold_rows'],flush=True)
 for name in totals:c.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({q(out/name/"part_*.parquet")})')
 species={name:c.execute(f'SELECT count(distinct lower(trim(scientificname))) FROM {name}').fetchone()[0] for name in totals}
 assert c.execute(f'SELECT count(*) FROM (SELECT lower(trim(scientificname)) k,count(*) n FROM accepted GROUP BY 1) WHERE n<{a.min_records}').fetchone()[0]==0
 assert c.execute(f'SELECT count(*) FROM (SELECT lower(trim(scientificname)) k,count(*) n FROM below_threshold GROUP BY 1) WHERE n>={a.min_records}').fetchone()[0]==0
 assert c.execute('SELECT count(*) FROM accepted a JOIN below_threshold b USING(source_row)').fetchone()[0]==0
 for name,pred in [('accepted',f'records>={a.min_records}'),('below_threshold',f'records<{a.min_records}')]:
  expected=c.execute(f'SELECT coalesce(sum(records),0),count(*) FROM species_counts WHERE {pred}').fetchone();assert expected==(totals[name],species[name])
 # Full-row identity and coverage was checked independently in each partition.
 c.execute(f'COPY (SELECT source_row,id,scientificname FROM below_threshold ORDER BY source_row) TO {q(out/"below_threshold_record_index.csv")} (HEADER)')
 threshold_comparison=c.execute('SELECT count(*) FILTER(WHERE records>=10),sum(records) FILTER(WHERE records>=10),count(*) FILTER(WHERE records BETWEEN 10 AND 14),sum(records) FILTER(WHERE records BETWEEN 10 AND 14) FROM species_counts').fetchone()
 summary=dict(input_rows=sum(totals.values()),input_species=sum(species.values()),minimum_records=a.min_records,accepted_rows=totals['accepted'],accepted_species=species['accepted'],below_threshold_rows=totals['below_threshold'],below_threshold_species=species['below_threshold'],minimum_retained_records=c.execute('SELECT min(n) FROM (SELECT count(*) n FROM accepted GROUP BY lower(trim(scientificname)))').fetchone()[0],ten_record_diagnostic=dict(zip(['species_at_least_10','rows_at_least_10','species_10_to_14','rows_10_to_14'],threshold_comparison)),original_fields_and_all_rows_verified=True,completed=True,preliminary_filter=True,environment_sampling_performed=False)
 manifest=dict(source=str(src),output=str(out),species_key='lower(trim(scientificname))',rule=f'Keep species with at least {a.min_records} rows after existing QC; no additional deduplication; preserve all fields; reassess count after environment validity filtering',python=platform.python_version(),duckdb=duckdb.__version__,script_sha256=sha(Path(__file__)))
 for fn,data in [('summary.json',summary),('manifest.json',manifest),('file_checksums.json',checks)]: (out/fn).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(summary),flush=True)
if __name__=='__main__':main()
