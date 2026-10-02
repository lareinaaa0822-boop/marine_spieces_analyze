"""Read-only comparison of saved model inputs and two depth sampling rules."""
from pathlib import Path
import json,re,hashlib
from collections import Counter
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window

out=Path('D:/IUCN_marine/rebuild_20260930/stored_environment_versions_20261002');out.mkdir(exist_ok=True)
variables=['thetao','so','chl','no3','o2']
indices={}; depths={}
for v in variables:
 d=Path('D:/ENV_TIF')/('PHY' if v in variables[:2] else 'ENV_TIFBGC_resampled')/v
 indices[v]={}; depths[v]={}
 for f in d.glob('*.tif'):
  m=re.fullmatch(rf'{v}_(\d{{4}})_L(\d+)_([\d.]+)m.tif',f.name)
  if m: indices[v][int(m[1]),int(m[2])]=f;depths[v][int(m[2])]=float(m[3])
def layers(z,v):
 a=np.array([depths[v][l] for l in range(1,51)])
 return np.abs(np.asarray(z)[:,None]-a[None,:]).argmin(axis=1)+1
roots={'presence':Path('G:/物种样本点处理后'),'background':Path('D:/背景点带环境值'),'training':Path('D:/MaxEnt输入'),'models':Path('D:/Maxent_rds'),'E_ENV_final':Path('E:/ENV_final')}
inventory={k:dict(path=str(p),csv_count=len(list(p.glob('*.csv'))),rds_count=len(list(p.glob('*.rds')))) for k,p in roots.items()}
common=sorted({f.stem for f in roots['models'].glob('*.rds')} & {f.stem for f in roots['training'].glob('*.csv')} & {f.stem for f in roots['presence'].glob('*.csv')})
# Reproducible species spread across the full alphabetical model list.
selected=[common[i] for i in np.unique(np.linspace(0,len(common)-1,24,dtype=int))]
for sp in ['Abalistes_stellatus','Abudefduf_saxatilis','Acanthastrea_echinata']:
 if sp in common and sp not in selected:selected.append(sp)
trace=[]; candidates=[]
for sp in selected:
 p=roots['presence']/(sp+'.csv');b=roots['background']/(sp+'_background.csv');t=roots['training']/(sp+'.csv')
 presence=pd.read_csv(p);training=pd.read_csv(t)
 background=pd.read_csv(b) if b.exists() else None
 rec=dict(species=sp,presence_rows=len(presence),training_presence_rows=int((training.pa==1).sum()),training_background_rows=int((training.pa==0).sum()),presence_sequence_exact=np.array_equal(presence[variables].to_numpy(),training.loc[training.pa==1,variables].to_numpy(),equal_nan=True),background_rows=len(background) if background is not None else None,background_sequence_exact=np.array_equal(background[variables].to_numpy(),training.loc[training.pa==0,variables].to_numpy(),equal_nan=True) if background is not None else None)
 trace.append(rec)
 for kind,df,path in [('presence',presence,p),('background',background,b)]:
  if df is None:continue
  df=df.copy();df['source_index']=df.index
  df=df.replace([np.inf,-np.inf],np.nan).dropna(subset=['depth','year','longitude','latitude'])
  df=df[(df.year>=1993)&(df.year<=2025)&(df.year==df.year.astype(int))]
  df['phy_layer']=layers(df.depth,'thetao');df['bgc_layer']=layers(df.depth,'chl')
  different=df[df.phy_layer!=df.bgc_layer];same=df[df.phy_layer==df.bgc_layer]
  take=pd.concat([different.sample(min(20,len(different)),random_state=20261002),same.sample(min(3,len(same)),random_state=20261002)])
  for _,r in take.iterrows():
   candidates.append(dict(species=sp,kind=kind,path=str(path),source_index=int(r.source_index),longitude=float(r.longitude),latitude=float(r.latitude),depth=float(r.depth),year=int(r.year),phy_layer=int(r.phy_layer),bgc_layer=int(r.bgc_layer),**{v:float(r[v]) for v in variables}))
 print('selected',sp,flush=True)
pd.DataFrame(trace).to_csv(out/'training_input_trace.csv',index=False,encoding='utf-8-sig')
# Group by raster to avoid repeatedly opening individual files.
samples=pd.DataFrame(candidates);values={}
for v in variables:
 jobs={}
 for i,r in samples.iterrows():
  for mode,l in [('shared_PHY',r.phy_layer),('independent',r.phy_layer if v in variables[:2] else r.bgc_layer)]:jobs.setdefault((int(r.year),int(l)),[]).append((i,mode,r.longitude,r.latitude))
 for (y,l),points in jobs.items():
  with rasterio.open(indices[v][y,l]) as src:
   for i,mode,x,yy in points:
    rr,cc=src.index(x,yy)
    val=int(src.read(1,window=Window(cc,rr,1,1))[0,0]) if 0<=rr<src.height and 0<=cc<src.width else None
    values[i,v,mode]=val
 print('reextracted',v,len(jobs),flush=True)
detail=[]
for i,r in samples.iterrows():
 for v in variables:
  a=values[i,v,'shared_PHY'];b=values[i,v,'independent'];old=r[v]
  detail.append(dict(**{k:r[k] for k in ['species','kind','path','source_index','longitude','latitude','depth','year','phy_layer','bgc_layer']},variable=v,stored=old,shared_PHY=a,independent=b,discriminating=a is not None and b is not None and a!=b,shared_match=a is not None and old==a,independent_match=b is not None and old==b))
df=pd.DataFrame(detail);df.to_csv(out/'pixel_reextraction_details.csv',index=False,encoding='utf-8-sig')
df.groupby(['kind','variable']).agg(comparisons=('stored','size'),discriminating=('discriminating','sum'),shared_matches=('shared_match','sum'),independent_matches=('independent_match','sum')).reset_index().to_csv(out/'all_comparisons_summary.csv',index=False,encoding='utf-8-sig')
dist=df[df.discriminating]
summary=dist.groupby(['kind','variable']).agg(discriminating=('stored','size'),shared_matches=('shared_match','sum'),independent_matches=('independent_match','sum')).reset_index()
summary.to_csv(out/'discriminating_comparisons_summary.csv',index=False,encoding='utf-8-sig')
s=dict(inventory=inventory,models_with_training_and_presence=len(common),checked_species=len(selected),point_rows=len(samples),training_presence_sequence_exact=sum(r['presence_sequence_exact'] for r in trace),training_background_sequence_exact=sum(r['background_sequence_exact'] is True for r in trace),discriminating_summary=summary.to_dict('records'),limitations=['Stratified sampled pixel re-extraction, not a full-row audit','Saved training CSV equality does not by itself prove a specific RDS was fitted from that CSV','Final manuscript model provenance requires distinguishing D models from newer E prediction products'])
(out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False),flush=True)
