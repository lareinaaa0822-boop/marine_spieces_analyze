"""Independent stratified sample audit against source records and frozen IUCN.
No production files modified. Python checks formula independently of SQL rules.
"""
import argparse,json,math
from pathlib import Path
import duckdb,pandas as pd
def main():
 p=argparse.ArgumentParser()
 for k in ['stage','source','iucn','raw-manifest']:p.add_argument('--'+k,required=True)
 a=p.parse_args();root=Path(a.stage)
 c=duckdb.connect();c.execute("SET memory_limit='2GB'");c.execute('SET threads=2')
 samples=[]
 for group in ['accepted','review','excluded']:
  for k in [0,31,63]:
   f=root/group/f'part_{k:03}.parquet'
   d=c.execute("SELECT * EXCLUDE(rn) FROM (SELECT *,row_number() OVER(PARTITION BY depth_qc_status ORDER BY source_row) rn FROM read_parquet(?)) WHERE rn<=5",[str(f)]).fetchdf()
   samples.append(d)
 df=pd.concat(samples,ignore_index=True)
 c.register('wanted',df[['source_row']])
 originals=c.execute('SELECT * FROM read_parquet(?) WHERE source_row IN (SELECT source_row FROM wanted)',[str(Path(a.source)/'*.parquet')]).fetchdf().set_index('source_row')
 names=pd.read_excel(a.iucn);names['key']=names.sci_name.astype(str).str.strip().str.lower()
 rawcols=json.loads(Path(a.raw_manifest).read_text(encoding='utf-8'))['columns']
 def eq(x,y):
  if pd.isna(x) and pd.isna(y):return True
  return x==y
 for _,r in df.iterrows():
  old=originals.loc[r.source_row]
  for col in rawcols:assert eq(r[col],old[col]),(r.source_row,col,'original changed')
  assert eq(r.depth_raw,old.depth)
  if r.depth_source=='observed':assert r.depth_final==float(old.depth) and r.depth_flag==0
  elif r.depth_source=='iucn_mid':
   assert str(old.depth).strip().lower() in ['', 'na','n/a','nan','null','none','<na>'] or pd.isna(old.depth)
   g=names[names.key==str(old.scientificname).strip().lower()]
   pairs=g[['depth_up','depth_low']].apply(pd.to_numeric,errors='coerce').drop_duplicates()
   assert len(pairs)==1
   up,low=pairs.iloc[0]
   assert math.isfinite(up) and math.isfinite(low) and 0<=up<=low<=11000
   assert r.depth_final==(up+low)/2 and r.depth_flag==1
  else:assert pd.isna(r.depth_final) and pd.isna(r.depth_flag)
  if r.depth_qc_status=='review_negative_observed':assert float(old.depth)<0
  if r.depth_qc_status=='review_iucn_reversed':assert r.iucn_depth_up>r.iucn_depth_low
  if r.depth_qc_status=='excluded_missing_depth_negative_endpoint':assert r.iucn_depth_up<0 or r.iucn_depth_low<0
 df.to_csv(root/'independent_depth_sample_records.csv',index=False,encoding='utf-8-sig')
 result=dict(sample_rows=len(df),source_rows_found=len(originals),counts=df.depth_qc_status.value_counts().to_dict(),all_original_fields_preserved=True,depth_formula_verified_against_original_iucn=True,passed=True)
 (root/'independent_sample_verification.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result))
if __name__=='__main__':main()
