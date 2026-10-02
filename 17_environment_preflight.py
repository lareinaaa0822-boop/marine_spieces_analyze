"""Read-only full raster-header preflight and current-point depth coverage.
Does not resolve source-to-raster correctness or sentinel ambiguity.
"""
import json,re,csv
from pathlib import Path
from collections import Counter
import rasterio,duckdb
out=Path('D:/IUCN_marine/rebuild_20260930/environment_preflight_20261002');out.mkdir(exist_ok=True)
dirs={v:Path('D:/ENV_TIF')/('PHY' if v in ['thetao','so'] else 'ENV_TIFBGC_resampled')/v for v in ['thetao','so','chl','no3','o2']}
rows=[];inventory=[];errors=[];reference=None;depths={}
for var,folder in dirs.items():
 seen=set();schemas={};unknown=[]
 for f in sorted(folder.glob('*.tif')):
  m=re.fullmatch(rf'{var}_(\d{{4}})_L(\d+)_([\d.]+)m\.tif',f.name)
  if not m:unknown.append(f.name);continue
  year,layer,z=int(m[1]),int(m[2]),float(m[3]);key=(year,layer)
  if key in seen:errors.append({'kind':'duplicate_key','variable':var,'key':key})
  seen.add(key);schemas.setdefault(layer,set()).add(z)
  try:
   with rasterio.open(f) as r:
    geometry=(r.width,r.height,tuple(r.transform),str(r.crs))
    if reference is None:reference=geometry
    match=geometry==reference
    if not match:errors.append({'kind':'grid_mismatch','path':str(f)})
    rows.append(dict(variable=var,year=year,layer=layer,filename_depth=z,path=str(f),bytes=f.stat().st_size,width=r.width,height=r.height,crs=str(r.crs),transform=repr(r.transform),dtype=r.dtypes[0],nodata=r.nodata,scale=r.scales[0],offset=r.offsets[0],band_count=r.count,grid_matches_reference=match))
  except Exception as e:errors.append({'kind':'unreadable','path':str(f),'error':str(e)})
 missing=sorted(set((y,l) for y in range(1993,2026) for l in range(1,51))-seen)
 varying={str(k):sorted(v) for k,v in schemas.items() if len(v)>1}
 depths[var]={k:next(iter(v)) for k,v in schemas.items() if len(v)==1}
 inventory.append(dict(variable=var,files=len(seen),missing=missing,varying_filename_depths=varying,unrecognized_names=unknown))
 print('header_check_complete',var,len(seen),flush=True)
with (out/'all_raster_headers.csv').open('w',newline='',encoding='utf-8-sig') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
layers=[dict(layer=l,**{v+'_depth_m':depths[v].get(l) for v in dirs}) for l in range(1,51)]
with (out/'verified_filename_depth_table.csv').open('w',newline='',encoding='utf-8-sig') as f:
 w=csv.DictWriter(f,fieldnames=list(layers[0]));w.writeheader();w.writerows(layers)
c=duckdb.connect();c.execute('SET threads=2')
maxz=max(depths['thetao'].values())
points=c.execute("SELECT count(*),count(*) FILTER(WHERE depth_final>?),count(*) FILTER(WHERE depth_final<0),min(depth_final),max(depth_final) FROM read_parquet('G:/Marine_AOH_rebuild_20260930/07_year_quality/accepted/*.parquet')",[maxz]).fetchone()
c.execute("COPY (SELECT source_row,id,scientificname,depth_raw,depth_final,depth_source,year_final FROM read_parquet('G:/Marine_AOH_rebuild_20260930/07_year_quality/accepted/*.parquet') WHERE depth_final>? ORDER BY source_row) TO '"+str(out/'points_below_deepest_layer.csv').replace("'","''")+"' (HEADER)",[maxz])
s=dict(inventory=inventory,raster_header_count=len(rows),errors=errors,header_patterns=[dict(dtype=d,nodata=n,scale=sc,offset=o,band_count=b,files=count) for (d,n,sc,o,b),count in Counter((x['dtype'],x['nodata'],x['scale'],x['offset'],x['band_count']) for x in rows).items()],reference_grid=reference,phy_bgc_equal_filename_depth_layers=sum(depths['thetao'][l]==depths['chl'][l] for l in range(1,51)),deepest_phy_depth_m=maxz,current_points=dict(zip(['rows','below_deepest_environment_rows','negative_depth_rows','minimum_depth','maximum_depth'],points)),limitations=['Headers cannot verify pixel scaling against original NetCDF','Temperature -1 sentinel ambiguity unresolved','Filename depths and generation script imply layer mapping but original 50-to-75 map not verified','Same transforms do not independently prove geolocation correct'])
(out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=True),flush=True)
