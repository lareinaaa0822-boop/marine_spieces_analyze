"""Diagnose archive differences without changing accepted data.

Strict comparison reproduces prior fingerprint rules. A second diagnostic view
equates literal NA in non-countryCode text fields with missing and rounds numeric
fields to 9 decimals. This sensitivity view is NOT a production dedup rule.
"""
import argparse,json
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def qi(s):return '"'+s.replace('"','""')+'"'
def main():
 p=argparse.ArgumentParser()
 for key in ['source','raw','archive','out']:p.add_argument('--'+key,required=True)
 a=p.parse_args();out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 c=duckdb.connect(str(out/'diagnostic.duckdb'));c.execute("SET memory_limit='5GB'");c.execute('SET threads=4');c.execute(f"SET temp_directory={q(out/'spill')}")
 keys=json.loads((Path(a.raw)/'manifest.json').read_text(encoding='utf-8'))['keys']
 texts={'scientificname','lifestage','sex','behavior','eventDate','country','countryCode','locality','continent','waterBody','habitat'}
 exprs=[[],[]]
 for col in keys:
  if col in texts:
   e=f"nullif(trim({qi(col)}),'')"
   if col=='scientificname':e=f'lower({e})'
   relaxed=e if col=='countryCode' else f"nullif({e},'NA')"
  else:
   n=f'try_cast({qi(col)} AS DOUBLE)';e=f'CASE WHEN isfinite({n}) THEN {n} ELSE NULL END';relaxed=f'round(({e}),9)'
  exprs[0].append(f'{qi(col)} := {e}');exprs[1].append(f'{qi(col)} := {relaxed}')
 sigs=['sha256(to_json(struct_pack('+','.join(es)+')))' for es in exprs]
 for name in ['new','old']:
  if (out/(name+'_done.json')).exists():continue
  if name=='new':
   c.execute('CREATE OR REPLACE TABLE new AS SELECT NULL::VARCHAR id,NULL::BIGINT source_row,NULL::VARCHAR species_key,NULL::VARCHAR strict_sig,NULL::VARCHAR relaxed_sig WHERE false')
   for part in sorted(Path(a.source).glob('*.parquet')):
    c.execute(f"INSERT INTO new SELECT id,source_row,lower(trim(scientificname)),{sigs[0]},{sigs[1]} FROM read_parquet({q(part)}) WHERE NOT (try_cast(longitude AS DOUBLE)=0 AND try_cast(latitude AS DOUBLE)=0)")
    print('diagnosed_new',part.name,flush=True)
  else:
   print('diagnose_archive_start',flush=True)
   c.execute(f"CREATE TABLE old AS SELECT id,lower(trim(scientificname)) species_key,{sigs[0]} strict_sig,{sigs[1]} relaxed_sig FROM read_csv({q(a.archive)},all_varchar=true,header=true,strict_mode=true)")
  (out/(name+'_done.json')).write_text(json.dumps({'rows':c.execute(f'SELECT count(*) FROM {name}').fetchone()[0]}))
  c.execute('CHECKPOINT');print(name,'complete',flush=True)
 results={}
 for mode in ['strict','relaxed']:
  for name in ['new','old']:
   c.execute(f'CREATE OR REPLACE TABLE {name}_{mode} AS SELECT {mode}_sig sig,count(*) n FROM {name} GROUP BY 1')
  r=c.execute(f'SELECT count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NOT NULL),count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NULL),count(*) FILTER(WHERE a.sig IS NULL AND b.sig IS NOT NULL),coalesce(sum(a.n) FILTER(WHERE b.sig IS NULL),0),coalesce(sum(b.n) FILTER(WHERE a.sig IS NULL),0) FROM new_{mode} a FULL OUTER JOIN old_{mode} b USING(sig)').fetchone()
  results[mode]=dict(zip(['common_groups','new_only_groups','old_only_groups','new_only_rows','old_only_rows'],r))
  for name in ['new','old']:
   results[mode][name+'_collapsed_rows']=c.execute(f'SELECT sum(n)-count(*) FROM {name}_{mode}').fetchone()[0]
  print(mode,json.dumps(results[mode]),flush=True)
 # List all unresolved IDs and relaxed-duplicate groups; these are audit outputs.
 for name,other in [('new','old'),('old','new')]:
  c.execute(f"COPY (SELECT a.* FROM {name} a ANTI JOIN {other}_relaxed b ON a.relaxed_sig=b.sig) TO {q(out/(name+'_unresolved_ids.csv'))} (HEADER)")
  c.execute(f"COPY (SELECT a.* FROM {name} a JOIN {name}_relaxed b ON a.relaxed_sig=b.sig WHERE b.n>1) TO {q(out/(name+'_relaxed_duplicate_ids.csv'))} (HEADER)")
 results['rules']={'strict':'36 source fields excluding ID, same prior canonical comparison','relaxed':'Diagnostic only: literal NA becomes missing in text except countryCode; numeric values rounded to 9 decimals. No data modified.'}
 results['completed']=True
 (out/'summary.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(results),flush=True)
if __name__=='__main__':main()
