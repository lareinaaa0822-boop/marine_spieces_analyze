"""Sample archive-only payloads by original ID to distinguish numeric round trips.
This is a bounded diagnostic, not a claim that all unmatched payloads have one cause.
"""
import argparse,json,math
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def i(s):return '"'+s.replace('"','""')+'"'

def main():
 p=argparse.ArgumentParser();p.add_argument('--raw',required=True);p.add_argument('--comparison',required=True);a=p.parse_args()
 raw,out=Path(a.raw),Path(a.comparison)
 c=duckdb.connect(str(out/'comparison.duckdb'),read_only=True);c.execute("SET memory_limit='3GB'");c.execute('SET threads=4')
 keys=json.loads((raw/'manifest.json').read_text(encoding='utf-8'))['keys']
 textcols={'scientificname','lifestage','sex','behavior','eventDate','country','countryCode','locality','continent','waterBody','habitat'}
 ex=[]
 for col in keys:
  if col in textcols:
   e=f"nullif(trim({i(col)}),'')"
   if col=='scientificname':e=f'lower({e})'
  else:
   n=f'try_cast({i(col)} AS DOUBLE)';e=f'CASE WHEN isfinite({n}) THEN {n} ELSE NULL END'
  ex.append(f'{i(col)} := {e}')
 sig='sha256(to_json(struct_pack('+','.join(ex)+')))'
 sources={'E_old_dedup':'E:/因物种重复造成的修改8月/数据/建模数据集_带深度标记_去重.csv',
          'E_strict_verified':'E:/重新做081/01_建模数据集_带深度标记_严格去重_验证版.csv'}
 samples=[]
 for name,f in sources.items():
  c.execute(f'CREATE OR REPLACE TEMP TABLE unmatched AS SELECT a.sig FROM {name} a ANTI JOIN new_sigs b USING(sig)')
  df=c.execute(f"SELECT * FROM read_csv({q(f)},all_varchar=true) WHERE {sig} IN (SELECT sig FROM unmatched) LIMIT 10").fetchdf()
  df['archive']=name;samples.append(df);print('sampled',name,len(df),flush=True)
 import pandas as pd
 old=pd.concat(samples,ignore_index=True);old.to_csv(out/'archive_difference_sample_old.csv',index=False,encoding='utf-8-sig')
 ids=old[['id']].drop_duplicates();c.register('sample_ids',ids)
 fields=['id']+keys
 # Materialize IDs first, then use the stable source-row range to restrict row groups.
 loc=c.execute(f'SELECT id,source_row,bucket FROM read_parquet({q(raw/"staging"/"*"/"*.parquet")}) WHERE id IN (SELECT id FROM sample_ids)').fetchdf()
 new_parts=[]
 for b,g in loc.groupby('bucket'):
  c.register('wanted',g[['source_row']])
  part=raw/'staging'/f'bucket={b}'/'*.parquet'
  lo,hi=int(g.source_row.min()),int(g.source_row.max())
  new_parts.append(c.execute(f'SELECT {",".join(map(i,fields))},source_row FROM read_parquet({q(part)}) WHERE source_row BETWEEN {lo} AND {hi} AND source_row IN (SELECT source_row FROM wanted)').fetchdf())
 new=pd.concat(new_parts,ignore_index=True) if new_parts else pd.DataFrame(columns=fields)
 new.to_csv(out/'archive_difference_sample_raw.csv',index=False,encoding='utf-8-sig')
 results=[]
 for _,r in old.iterrows():
  matches=new[new.id==r.id]
  entry={'archive':r.archive,'id':r.id,'raw_id_matches':len(matches),'differences':[]}
  if len(matches)==1:
   nr=matches.iloc[0]
   for col in keys:
    x,y=nr[col],r[col]
    x='' if pd.isna(x) else str(x);y='' if pd.isna(y) else str(y)
    if x==y:continue
    diff={'field':col,'raw':x,'archive':y}
    try:
     fx,fy=float(x),float(y);diff['numeric_abs_difference']=abs(fx-fy)
     diff['within_1e_minus9']=bool(math.isfinite(fx) and math.isfinite(fy) and math.isclose(fx,fy,rel_tol=0,abs_tol=1e-9))
    except ValueError:diff['within_1e_minus9']=x.strip()==y.strip()
    entry['differences'].append(diff)
  entry['all_examined_differences_within_tolerance']=len(matches)==1 and all(x['within_1e_minus9'] for x in entry['differences'])
  results.append(entry)
 (out/'archive_difference_sample_diagnosis.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
 print('diagnosis_complete',sum(x['all_examined_differences_within_tolerance'] for x in results),'of',len(results),flush=True)

if __name__=='__main__':main()
