"""Read-only historical comparison of the IUCN intersection stage.

Reuse the prior verified archive fingerprint database. Signatures compare the
36 source fields except ID; derived depth fields are excluded. This establishes
payload equality under the documented normalization, not ecological equivalence.
"""
import argparse,json,hashlib
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def qi(s):return '"'+s.replace('"','""')+'"'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
 return h.hexdigest()

def main():
 p=argparse.ArgumentParser()
 for key in ['stage','raw','archive-db','out']:p.add_argument('--'+key,required=True)
 a=p.parse_args();stage,raw,out=Path(a.stage),Path(a.raw),Path(a.out)
 out.mkdir(parents=True,exist_ok=False);(out/'parts').mkdir()
 c=duckdb.connect(str(out/'audit.duckdb'));c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 c.execute(f"ATTACH {q(a.archive_db)} AS historical (READ_ONLY)")
 keys=json.loads((raw/'manifest.json').read_text(encoding='utf-8'))['keys']
 texts={'scientificname','lifestage','sex','behavior','eventDate','country','countryCode','locality','continent','marine_text','waterBody','habitat'}
 ex=[]
 for col in keys:
  if col in texts:
   e=f"nullif(trim({qi(col)}),'')"
   if col=='scientificname':e=f'lower({e})'
  else:
   n=f'try_cast({qi(col)} AS DOUBLE)';e=f'CASE WHEN isfinite({n}) THEN {n} ELSE NULL END'
  ex.append(f'{qi(col)} := {e}')
 sig='sha256(to_json(struct_pack('+','.join(ex)+')))'
 files=[]
 for part in sorted((stage/'accepted').glob('*.parquet')):
  c.execute(f"COPY (SELECT {sig} AS sig,lower(trim(scientificname)) species_key,count(*) n FROM read_parquet({q(part)}) GROUP BY 1,2) TO {q(out/'parts'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
  files.append(dict(file=str(part),bytes=part.stat().st_size,sha256=sha(part)))
  print('fingerprinted',part.name,flush=True)
 c.execute(f"CREATE TABLE current_sigs AS SELECT sig,species_key,sum(n)::BIGINT n FROM read_parquet({q(out/'parts'/'*.parquet')}) GROUP BY 1,2")
 c.execute('CREATE TABLE counts_new AS SELECT species_key,sum(n)::BIGINT n FROM current_sigs GROUP BY 1')
 result={'stage_summary':json.loads((stage/'summary.json').read_text(encoding='utf-8')),'comparison_rule':'SHA256 of 36 original non-ID fields. Text trimmed, empty->NULL, species lowercased; numeric finite DOUBLE else NULL. Literal NA text remains literal; no rounding tolerance. Derived IUCN/depth fields excluded.','archives':{}}
 for name in ['E_old_dedup','E_strict_verified']:
  old='historical.'+name
  c.execute(f'CREATE OR REPLACE TEMP TABLE counts_old AS SELECT species_key,sum(n)::BIGINT n FROM {old} GROUP BY 1')
  c.execute('CREATE OR REPLACE TEMP TABLE species_diff AS SELECT coalesce(a.species_key,b.species_key) species_key,coalesce(a.n,0)::BIGINT new_rows,coalesce(b.n,0)::BIGINT archive_rows,(coalesce(a.n,0)-coalesce(b.n,0))::BIGINT delta FROM counts_new a FULL OUTER JOIN counts_old b USING(species_key)')
  c.execute(f"COPY (SELECT * FROM species_diff ORDER BY abs(delta) DESC,species_key) TO {q(out/(name+'_species_counts.csv'))} (HEADER)")
  species=c.execute('SELECT count(*) FILTER(WHERE new_rows>0 AND archive_rows>0),count(*) FILTER(WHERE new_rows>0 AND archive_rows=0),count(*) FILTER(WHERE new_rows=0 AND archive_rows>0),count(*) FILTER(WHERE new_rows=archive_rows),count(*) FILTER(WHERE new_rows<>archive_rows),sum(new_rows),sum(archive_rows) FROM species_diff').fetchone()
  payload=c.execute(f'SELECT count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NOT NULL),count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NULL),count(*) FILTER(WHERE a.sig IS NULL AND b.sig IS NOT NULL) FROM current_sigs a FULL OUTER JOIN {old} b USING(sig)').fetchone()
  result['archives'][name]=dict(zip(['shared_species','new_only_species','archive_only_species','species_equal_counts','species_different_counts','new_rows','archive_rows'],species))
  result['archives'][name].update(dict(zip(['common_unique_payloads','new_only_unique_payloads','archive_only_unique_payloads'],payload)))
  result['archives'][name]['top_count_differences']=c.execute('SELECT * FROM species_diff ORDER BY abs(delta) DESC LIMIT 10').fetchall()
 result['new_normalized_duplicate_rows']=c.execute('SELECT sum(n)-count(*) FROM current_sigs').fetchone()[0]
 result['completed']=True
 (out/'comparison_summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
 (out/'output_file_sha256.json').write_text(json.dumps(files,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(result,ensure_ascii=True),flush=True)

if __name__=='__main__':main()
