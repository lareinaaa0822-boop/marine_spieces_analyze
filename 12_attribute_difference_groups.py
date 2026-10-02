"""Inspect every candidate duplicate group and residual record from diagnostic 11.
Only audit artifacts are written. Never deduplicate or normalize production data.
"""
import argparse,json,math
from collections import Counter,defaultdict
from pathlib import Path
import duckdb,pandas as pd

def main():
 p=argparse.ArgumentParser()
 for k in ['source','archive','diagnosis','raw']:p.add_argument('--'+k,required=True)
 p.add_argument('--reuse-extracted',action='store_true')
 a=p.parse_args();out=Path(a.diagnosis)
 keys=json.loads((Path(a.raw)/'manifest.json').read_text(encoding='utf-8'))['keys']
 texts={'scientificname','lifestage','sex','behavior','eventDate','country','countryCode','locality','continent','waterBody','habitat'}
 c=duckdb.connect();c.execute("SET memory_limit='3GB'");c.execute('SET threads=3')
 data={};metadata={}
 for name in ['new','old']:
  dup=pd.read_csv(out/(name+'_relaxed_duplicate_ids.csv'),dtype=str,keep_default_na=False)
  rem=pd.read_csv(out/(name+'_unresolved_ids.csv'),dtype=str,keep_default_na=False)
  wanted=pd.concat([dup,rem]).drop_duplicates('id');metadata[name]=(dup,rem)
  c.register('wanted',wanted)
  cached=out/(name+'_group_and_residual_records.csv')
  if a.reuse_extracted and cached.exists():
   df=pd.read_csv(cached,dtype=str,keep_default_na=False)
  elif name=='new':
   df=c.execute("SELECT * FROM read_parquet(?) WHERE source_row IN (SELECT try_cast(source_row AS BIGINT) FROM wanted)",[str(Path(a.source)/'*.parquet')]).fetchdf()
  else:
   df=c.execute('SELECT * FROM read_csv(?,all_varchar=true,header=true) WHERE id IN (SELECT id FROM wanted)',[a.archive]).fetchdf()
  df=df.fillna('');df.to_csv(out/(name+'_group_and_residual_records.csv'),index=False,encoding='utf-8-sig');data[name]=df
  print('records_loaded',name,len(df),flush=True)
 def norm(col,val,na=False,rounded=False,extended=False):
  val=str(val).strip()
  if col in texts:
   if not val:return None
   if col=='scientificname':return val.lower()
   if extended and col=='behavior':
    try:
     v=float(val)
     if math.isfinite(v):return ('numeric_behavior_code',v)
    except ValueError:pass
   tokens={'NA'} if not extended else {'NA','None','nan','NaN','N/A','NULL','null','<NA>','#N/A','#NA','-NaN','-nan','n/a'}
   if na and col!='countryCode' and val in tokens:return None
   return val
  try:
   v=float(val)
   if not math.isfinite(v):return None
   return round(v,9) if rounded else v
  except ValueError:return None
 def key(r,na=False,rounded=False,extended=False):return tuple(norm(k,r[k],na,rounded,extended) for k in keys)
 result={}
 for name in ['new','old']:
  dup,rem=metadata[name];df=data[name]
  lookup={r['id']:r for r in df.to_dict('records')}
  fields=Counter();combinations=Counter();collapsed_na=collapsed_round=collapsed_combined=0;groups=[]
  for sig,g in dup.groupby('relaxed_sig'):
   rows=[lookup[x] for x in g.id]
   n=len(rows);collapsed_na+=n-len({key(r,na=True) for r in rows});collapsed_round+=n-len({key(r,rounded=True) for r in rows});collapsed_combined+=n-len({key(r,na=True,rounded=True) for r in rows})
   diff=[col for col in keys if len({norm(col,r[col]) for r in rows})>1]
   fields.update(diff);combinations.update([';'.join(diff)])
   groups.append(dict(relaxed_sig=sig,rows=n,different_fields=';'.join(diff)))
  pd.DataFrame(groups).to_csv(out/(name+'_duplicate_group_causes.csv'),index=False,encoding='utf-8-sig')
  result[name]=dict(duplicate_groups=len(groups),candidate_duplicate_rows=len(dup),collapsed_na_only=collapsed_na,collapsed_numeric_round_only=collapsed_round,collapsed_combined=collapsed_combined,fields_differing_across_groups=dict(fields),field_combinations=dict(combinations))
 # Match residual pairs under extended missing tokens; do not change countryCode.
 old_rows=data['old'][data['old'].id.isin(metadata['old'][1].id)].to_dict('records')
 new_rows=data['new'][data['new'].id.isin(metadata['new'][1].id)].to_dict('records')
 oldindex=defaultdict(list)
 for r in old_rows:oldindex[key(r,na=True,rounded=True,extended=True)].append(r)
 details=[];unmatched=[]
 for r in new_rows:
  matches=oldindex.get(key(r,na=True,rounded=True,extended=True),[])
  if len(matches)!=1:
   unmatched.append(r);continue
  old=matches[0]
  changes=[]
  for col in keys:
   if norm(col,r[col])!=norm(col,old[col]):changes.append({'field':col,'new':str(r[col]),'old':str(old[col])})
  details.append(dict(new_id=r['id'],old_id=old['id'],species=r['scientificname'],differences=changes))
 (out/'residual_pair_differences.json').write_text(json.dumps(details,ensure_ascii=False,indent=2),encoding='utf-8')
 pd.DataFrame(unmatched).to_csv(out/'still_unmatched_new_records.csv',index=False,encoding='utf-8-sig')
 result['residual']=dict(new_rows=len(new_rows),old_rows=len(old_rows),extended_missing_and_behavior_format_unique_matches=len(details),still_unmatched=len(unmatched),difference_fields=dict(Counter(x['field'] for row in details for x in row['differences'])))
 (out/'attribution_summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(result,ensure_ascii=True),flush=True)
if __name__=='__main__':main()
