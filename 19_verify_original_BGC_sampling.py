from pathlib import Path
import pandas as pd,numpy as np,rasterio,json
from rasterio.windows import Window
p=Path('D:/IUCN_marine/rebuild_20260930/stored_environment_versions_20261002')
d=pd.read_csv(p/'pixel_reextraction_details.csv');d=d[d.variable.isin(['chl','no3','o2'])].copy()
for i,r in d.iterrows():
 for variant,root in [('original_BGC',Path('D:/ENV_TIF/BGC')),('resampled',Path('D:/ENV_TIF/ENV_TIFBGC_resampled'))]:
  matches=[];center={}
  for layer in sorted({int(r.phy_layer),int(r.bgc_layer)}):
   f=list((root/r.variable).glob(f'{r.variable}_{int(r.year)}_L{layer}_*.tif'))
   if not f:continue
   with rasterio.open(f[0]) as src:
    rr,cc=src.index(r.longitude,r.latitude);a=src.read(1,window=Window(cc-1,rr-1,3,3))
    if a.shape==(3,3):
     center[str(layer)]=int(a[1,1]);matches.extend([[layer,int(x)-1,int(y)-1] for x,y in zip(*np.where(a==r.stored))])
  d.at[i,variant+'_centers']=json.dumps(center);d.at[i,variant+'_neighbor_matches']=json.dumps(matches)
d.to_csv(p/'residual_pixel_checks.csv',index=False,encoding='utf-8-sig')
for i,r in d.iterrows():
 centers=json.loads(r.original_BGC_centers)
 d.at[i,'original_BGC_shared']=centers.get(str(int(r.phy_layer)))
 d.at[i,'original_BGC_independent']=centers.get(str(int(r.bgc_layer)))
d['original_shared_match']=d.stored==d.original_BGC_shared
d['original_independent_match']=d.stored==d.original_BGC_independent
d['original_discriminating']=d.original_BGC_shared!=d.original_BGC_independent
d.to_csv(p/'original_BGC_pixel_checks.csv',index=False,encoding='utf-8-sig')
print(d.groupby(['kind','variable']).agg(n=('stored','size'),original_shared=('original_shared_match','sum'),original_independent=('original_independent_match','sum')).to_string())
print(d[d.original_discriminating].groupby(['kind','variable']).agg(n=('stored','size'),original_shared=('original_shared_match','sum'),original_independent=('original_independent_match','sum')).to_string())
