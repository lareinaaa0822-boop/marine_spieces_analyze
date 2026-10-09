"""REBUILD point-key deduplication, not the first-version historical pipeline.

This script produced stage 09 under the user's earlier approved rules.
Do not use it to claim reproduction of the first 5,619-model workflow:
the first version applied an early species n>=15 filter before year handling,
and its saved spatial outputs still contain repeated point keys.
Preserve source fields; prefer observed depth, then earliest source_row.
"""
import argparse,json,hashlib,platform,time
from pathlib import Path
import duckdb
def spatial_filter(source,out):
 import pandas as pd
 source,out=Path(source),Path(out)
 audit=Path('D:/IUCN_marine/rebuild_20260930/spatial_preflight_20261003')
 s=json.loads((audit/'summary.json').read_text(encoding='utf-8'));assert s['completed'] and not s['issues']
 for row in pd.read_csv(audit/'buffer_file_inventory.csv').itertuples():assert sha(Path(row.path))==row.sha256
 hist=pd.read_csv(audit/'boundary_species_historical_check.csv');assert hist.loc[hist['mode']=='contains','exact_records_equal'].all()
 out.mkdir(parents=True,exist_ok=False)
 groups=['accepted','outside_buffer','boundary_excluded','below_threshold','review_missing_buffer']
 for k in groups:(out/k).mkdir()
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 c.execute(f'CREATE TABLE decisions AS SELECT * FROM read_parquet({q(audit/"current_buffer_decisions.parquet")})')
 c.execute('CREATE TABLE counts AS SELECT species_key,count(*) FILTER(WHERE buffer_inside) spatial_records FROM decisions GROUP BY 1')
 assert c.execute('SELECT count(*),count(distinct source_row) FROM decisions').fetchone()==(5800207,5800207)
 c.execute(f'COPY (SELECT * FROM counts ORDER BY species_key) TO {q(out/"species_spatial_counts.csv")} (HEADER)')
 checks=[];totals={k:0 for k in groups}
 for p in sorted(source.glob('part_*.parquet')):
  c.execute(f'CREATE OR REPLACE TEMP TABLE part AS SELECT * FROM read_parquet({q(p)})')
  c.execute('CREATE OR REPLACE TEMP TABLE result AS SELECT p.*,d.buffer_status,d.buffer_inside,d.buffer_boundary,n.spatial_records FROM part p JOIN decisions d USING(source_row) JOIN counts n ON d.species_key=n.species_key')
  predicates=dict(accepted="buffer_status='ready' AND buffer_inside AND spatial_records>=15",outside_buffer="buffer_status='ready' AND NOT buffer_inside AND NOT buffer_boundary",boundary_excluded="buffer_status='ready' AND buffer_boundary",below_threshold="buffer_status='ready' AND buffer_inside AND spatial_records<15",review_missing_buffer="buffer_status<>'ready'")
  check=dict(part=p.name,input_sha256=sha(p),input_rows=c.execute('SELECT count(*) FROM part').fetchone()[0])
  for k,pred in predicates.items():
   dest=out/k/p.name;c.execute(f'COPY (SELECT * FROM result WHERE {pred}) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)')
   n=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0];totals[k]+=n;check[k]=n;check[k+'_sha256']=sha(dest)
  assert sum(check[k] for k in groups)==check['input_rows']
  cols=','.join('"'+r[0].replace('"','""')+'"' for r in c.execute('DESCRIBE part').fetchall())
  union=' UNION ALL '.join(f'SELECT {cols} FROM read_parquet({q(out/k/p.name)})' for k in groups)
  assert c.execute(f'SELECT count(*) FROM part p FULL JOIN ({union}) o USING(source_row) WHERE p IS DISTINCT FROM o').fetchone()[0]==0
  checks.append(check);print('spatial_partition',p.name,flush=True)
 assert totals['accepted']==s['predicted_rows_ge15'] and sum(totals.values())==5800207
 result=dict(completed=True,input_rows=5800207,input_species=5209,accepted_species=s['predicted_species_ge15'],**{k+'_rows':v for k,v in totals.items()},all_original_fields_verified=True,buffer_predicate='contains: boundary excluded',buffer_root='D:/IUCN_marine',missing_buffer_species=29,environment_rules_unchanged=True,remaining_environment_outside_rows=c.execute(f'SELECT count(*) FROM read_parquet({q(out/"accepted"/"*.parquet")}) WHERE environment_outside_mask<>0').fetchone()[0])
 for name,data in [('summary.json',result),('file_checksums.json',checks),('manifest.json',dict(source=str(source),audit=str(audit),decisions_sha256=sha(audit/'current_buffer_decisions.parquet'),script_sha256=sha(Path(__file__)),rule='existing species buffer contains -> species >=15; missing buffer quarantined; no observed>=10 rule'))]:
  (out/name).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(result),flush=True)
def environment_first(source,out):
 """Restore ALL deduplicated records, sample legacy rasters, then filter n>=15."""
 import re
 import numpy as np
 import pandas as pd
 import rasterio
 source,out=Path(source),Path(out)
 prior=json.loads((source/'summary.json').read_text(encoding='utf-8'))
 assert prior['completed'] and prior['deduplicated_rows']==12260028
 out.mkdir(parents=True,exist_ok=False)
 for folder in ['accepted','environment_excluded','below_threshold','checks']:(out/folder).mkdir()
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 paths=[str(source/k/'part_*.parquet') for k in ['accepted','below_threshold']]
 paths_sql='['+','.join(q(p) for p in paths)+']'
 c.execute(f'CREATE VIEW input AS SELECT * FROM read_parquet({paths_sql})')
 pts=c.execute('SELECT source_row,try_cast(longitude AS DOUBLE) longitude,try_cast(latitude AS DOUBLE) latitude,depth_final,year_final FROM input ORDER BY source_row').fetchdf()
 assert len(pts)==12260028 and pts.source_row.is_unique and not pts.isna().any().any()
 variables=['thetao','so','chl','no3','o2'];indices={};depths={};inventory=[]
 for v in variables:
  root=Path('D:/ENV_TIF')/('PHY' if v in variables[:2] else 'BGC')/v
  indices[v]={};depths[v]={}
  for f in sorted(root.glob('*.tif')):
   m=re.fullmatch(rf'{v}_(\d{{4}})_L(\d+)_([\d.]+)m.tif',f.name)
   if m:
    y,l,z=int(m[1]),int(m[2]),float(m[3]);assert (y,l) not in indices[v];indices[v][y,l]=f
    if l in depths[v]:assert depths[v][l]==z
    depths[v][l]=z
  assert len(indices[v])==1650 and set(depths[v])==set(range(1,51))
 def nearest(z,v):
  # Strict '<' gives the shallower/lower layer the tie, matching sorted argmin.
  best=np.full(len(z),np.inf);assigned=np.zeros(len(z),dtype=np.int16)
  for layer in sorted(depths[v]):
   delta=np.abs(z-depths[v][layer]);mask=delta<best;assigned[mask]=layer;best[mask]=delta[mask]
  return assigned
 z=pts.depth_final.to_numpy();year=pts.year_final.to_numpy(dtype=np.int32)
 phy=nearest(z,'thetao');bgc=nearest(z,'chl')
 values=pd.DataFrame({'source_row':pts.source_row,'phy_layer':phy,'bgc_layer':bgc})
 outside=np.zeros(len(pts),dtype=np.uint8)
 manifest=dict(source=str(source),input_sets=['accepted','below_threshold'],input_records=len(pts),workflow='sample all deduplicated records -> exclude any -1/null -> species >=15',variables=variables,phy_root='D:/ENV_TIF/PHY',bgc_root='D:/ENV_TIF/BGC',depth_rule='independent nearest physical depth per PHY/BGC; lowest layer on ties',scale='stored integer values without rescaling',legacy_out_of_bounds='rasterio.sample default: nodata if defined, otherwise 0; flagged separately; zero is not automatically rejected',environment_files_unchanged=True,script_sha256=sha(Path(__file__)))
 (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
 x=pts.longitude.to_numpy();y=pts.latitude.to_numpy()
 for vi,v in enumerate(variables):
  layer=phy if vi<2 else bgc;key=year*100+layer;order=np.argsort(key,kind='stable');ordered=key[order];starts=np.r_[0,np.flatnonzero(np.diff(ordered))+1,len(order)]
  result=np.full(len(pts),np.nan,dtype=np.float64)
  for j in range(len(starts)-1):
   ids=order[starts[j]:starts[j+1]];code=int(ordered[starts[j]]);yr,ln=divmod(code,100);path=indices[v][yr,ln]
   with rasterio.open(path) as r:
    transform=r.transform;assert transform.b==0 and transform.d==0 and r.count==1
    rr=np.floor((y[ids]-transform.f)/transform.e).astype(np.int64);cc=np.floor((x[ids]-transform.c)/transform.a).astype(np.int64)
    valid=(rr>=0)&(rr<r.height)&(cc>=0)&(cc<r.width)
    fallback=r.nodata if r.nodata is not None else 0
    result[ids]=fallback;outside[ids[~valid]]|=np.uint8(1<<vi)
    array=r.read(1);result[ids[valid]]=array[rr[valid],cc[valid]]
    # Verify vector extraction against the historical rasterio.sample API.
    sample=ids[np.linspace(0,len(ids)-1,min(3,len(ids)),dtype=int)]
    ref=np.array([a[0] for a in r.sample(zip(x[sample],y[sample]))],dtype=float)
    assert np.array_equal(result[sample],ref,equal_nan=True)
    inventory.append(dict(variable=v,year=yr,layer=ln,path=str(path),bytes=path.stat().st_size,mtime_ns=path.stat().st_mtime_ns,sampled_rows=len(ids)))
   if j%200==0:print('sampling',v,j+1,len(starts)-1,flush=True)
  values[v]=result;print('variable_complete',v,flush=True)
  del result,order,ordered,key
 values['environment_outside_mask']=outside
 mask=np.zeros(len(pts),dtype=np.uint8)
 for vi,v in enumerate(variables):mask[(values[v].to_numpy()==-1)|~np.isfinite(values[v].to_numpy())]|=np.uint8(1<<vi)
 values['environment_invalid_mask']=mask
 values.to_parquet(out/'environment_values.parquet',index=False,compression='zstd')
 pd.DataFrame(inventory).to_csv(out/'raster_sources.csv',index=False,encoding='utf-8-sig')
 c.execute(f'CREATE VIEW env AS SELECT * FROM read_parquet({q(out/"environment_values.parquet")})')
 assert c.execute('SELECT count(*),count(distinct source_row) FROM env').fetchone()==(len(pts),len(pts))
 del values,pts,x,y,z,year,phy,bgc,mask,outside
 c.execute('CREATE TABLE counts AS SELECT lower(trim(p.scientificname)) species_key,count(*) input_records,count(*) FILTER(WHERE e.environment_invalid_mask=0) valid_records FROM input p JOIN env e USING(source_row) GROUP BY 1')
 c.execute(f'COPY (SELECT *,valid_records>=15 retained FROM counts ORDER BY species_key) TO {q(out/"species_counts_before_after_environment.csv")} (HEADER)')
 totals={k:0 for k in ['accepted','environment_excluded','below_threshold']};checks=[]
 for i in range(64):
  files=[source/k/f'part_{i:03d}.parquet' for k in ['accepted','below_threshold']]
  c.execute('CREATE OR REPLACE TEMP VIEW part AS SELECT * FROM read_parquet(['+','.join(q(p) for p in files)+'])')
  c.execute('CREATE OR REPLACE TEMP TABLE result AS SELECT p.*,e.* EXCLUDE(source_row),s.valid_records species_environment_valid_records FROM part p JOIN env e USING(source_row) JOIN counts s ON lower(trim(p.scientificname))=s.species_key')
  preds={'accepted':'environment_invalid_mask=0 AND species_environment_valid_records>=15','environment_excluded':'environment_invalid_mask<>0','below_threshold':'environment_invalid_mask=0 AND species_environment_valid_records<15'}
  check=dict(part=i,input_files=[dict(path=str(f),sha256=sha(f)) for f in files],input_rows=c.execute('SELECT count(*) FROM part').fetchone()[0])
  for name,pred in preds.items():
   dest=out/name/f'part_{i:03d}.parquet';c.execute(f'COPY (SELECT * FROM result WHERE {pred}) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)')
   n=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0];totals[name]+=n;check[name]=n;check[name+'_sha256']=sha(dest)
  assert sum(check[k] for k in totals)==check['input_rows']
  cols=[r[0] for r in c.execute('DESCRIBE part').fetchall()];fields=','.join('"'+s.replace('"','""')+'"' for s in cols)
  combined=' UNION ALL '.join(f'SELECT {fields} FROM read_parquet({q(out/k/f"part_{i:03d}.parquet")})' for k in totals)
  assert c.execute(f'SELECT count(*) FROM part p FULL JOIN ({combined}) o ON p.source_row=o.source_row WHERE p IS DISTINCT FROM o').fetchone()[0]==0
  check['original_fields_verified']=True;checks.append(check);print('output_partition',i,flush=True)
 s=dict(completed=True,input_rows=sum(totals.values()),input_species=c.execute('SELECT count(*) FROM counts').fetchone()[0],**{k+'_rows':v for k,v in totals.items()},accepted_species=c.execute('SELECT count(*) FROM counts WHERE valid_records>=15').fetchone()[0],environment_valid_rows=totals['accepted']+totals['below_threshold'],minimum_records=15,original_fields_verified=True,out_of_bounds_rows=c.execute('SELECT count(*) FROM env WHERE environment_outside_mask<>0').fetchone()[0],legacy_zero_outside_retained_rows=c.execute('SELECT count(*) FROM env WHERE environment_outside_mask<>0 AND environment_invalid_mask=0').fetchone()[0])
 assert s['input_rows']==12260028
 c.execute(f'COPY (SELECT environment_invalid_mask,count(*) records FROM env GROUP BY 1 ORDER BY 1) TO {q(out/"exclusion_reason_masks.csv")} (HEADER)')
 for fn,data in [('summary.json',s),('file_checksums.json',checks)]: (out/fn).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(s),flush=True)
def q(p):return "'"+str(p).replace("'","''")+"'"
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--source',required=True);ap.add_argument('--out',required=True);ap.add_argument('--workflow',choices=['approved-rebuild-point-key','first-version','environment-first','spatial-filter'],required=True,help='Explicitly identify the workflow.');a=ap.parse_args()
 if a.workflow=='spatial-filter':return spatial_filter(a.source,a.out)
 if a.workflow=='environment-first':return environment_first(a.source,a.out)
 if a.workflow=='first-version':
  ap.error('First-version reproduction must not execute this extra point-key dedup stage. Its base-rule conflicts and intermediate spatial step must be resolved in the existing workflow before processing; see first_version_order_audit_20261002.')
 src,out=Path(a.source),Path(a.out)
 if out.exists():assert not list(out.rglob('part_*.parquet')) and not (out/'summary.json').exists(), 'Refusing to overwrite existing results'
 out.mkdir(parents=True,exist_ok=True)
 for d in ['accepted','below_threshold','duplicate_removed','checks']:(out/d).mkdir(exist_ok=True)
 c=duckdb.connect(str(out/'decisions.duckdb'));c.execute("SET memory_limit='8GB'");c.execute('SET threads=4');c.execute(f'SET temp_directory={q(out/"spill")}')
 c.execute(f'CREATE OR REPLACE VIEW source AS SELECT * FROM read_parquet({q(src/"part_*.parquet")})')
 c.execute('''CREATE TABLE keys AS SELECT source_row,lower(trim(scientificname)) species_key,try_cast(longitude AS DOUBLE) lon,try_cast(latitude AS DOUBLE) lat,depth_final depth,year_final year_key,cast(depth_flag AS INTEGER) priority FROM source''')
 assert c.execute("SELECT count(*) FROM keys WHERE source_row IS NULL OR species_key IS NULL OR species_key='' OR lon IS NULL OR lat IS NULL OR depth IS NULL OR year_key IS NULL OR priority IS NULL OR priority NOT IN (0,1) OR NOT isfinite(lon) OR NOT isfinite(lat) OR NOT isfinite(depth)").fetchone()[0]==0
 n,unique_ids=c.execute('SELECT count(*),count(distinct source_row) FROM keys').fetchone();assert n==unique_ids
 print('keys_ready',n,flush=True)
 c.execute('''CREATE TABLE groups AS SELECT species_key,lon,lat,depth,year_key,count(*) group_size,min(priority) best_priority,arg_min(source_row,struct_pack(priority:=priority,source_row:=source_row)) keep_source_row FROM keys GROUP BY species_key,lon,lat,depth,year_key''')
 c.execute('''CREATE TABLE decisions AS SELECT k.source_row,g.keep_source_row,g.group_size,k.species_key,k.priority,g.best_priority FROM keys k JOIN groups g USING(species_key,lon,lat,depth,year_key)''')
 assert c.execute('SELECT count(*),count(distinct source_row) FROM decisions').fetchone()==(n,n)
 assert c.execute('SELECT count(*) FROM decisions WHERE source_row=keep_source_row AND priority<>best_priority').fetchone()[0]==0
 c.execute('CREATE TABLE species_counts AS SELECT species_key,count(*) unique_points,sum(group_size) input_records FROM groups GROUP BY species_key')
 c.execute(f"COPY (SELECT *,unique_points>=15 retained FROM species_counts ORDER BY species_key) TO {q(out/'species_counts_before_after.csv')} (HEADER)")
 c.execute(f"COPY (SELECT source_row,keep_source_row,group_size,species_key,priority,best_priority FROM decisions WHERE source_row<>keep_source_row ORDER BY source_row) TO {q(out/'duplicate_record_mapping.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
 counts=c.execute('SELECT sum(input_records),sum(unique_points),count(*),sum(unique_points) FILTER(WHERE unique_points>=15),count(*) FILTER(WHERE unique_points>=15),sum(unique_points) FILTER(WHERE unique_points<15),count(*) FILTER(WHERE unique_points<15) FROM species_counts').fetchone()
 print('group_counts',counts,flush=True)
 manifest=dict(source=str(src),species_key='lower(trim(scientificname))',point_key=['species_key','longitude as DOUBLE','latitude as DOUBLE','depth_final','year_final'],rounding=False,priority=['depth_flag=0 before depth_flag=1','smallest source_row'],minimum_unique_points=15,source_fields_unchanged=True,source_is_year_stage_before_provisional_species_filter=True,environment_or_spatial_filtering=False,python=platform.python_version(),duckdb=duckdb.__version__,script_sha256=sha(Path(__file__)))
 (out/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
 totals=dict(accepted=0,below_threshold=0,duplicate_removed=0);checks=[]
 parts=sorted(src.glob('part_*.parquet'));assert len(parts)==64
 for p in parts:
  c.execute(f'CREATE OR REPLACE TEMP VIEW part AS SELECT * FROM read_parquet({q(p)})')
  c.execute('CREATE OR REPLACE TEMP TABLE selected AS SELECT d.source_row,d.keep_source_row,s.unique_points FROM part p JOIN decisions d USING(source_row) JOIN species_counts s USING(species_key)')
  predicates={'accepted':'source_row=keep_source_row AND unique_points>=15','below_threshold':'source_row=keep_source_row AND unique_points<15','duplicate_removed':'source_row<>keep_source_row'}
  chk=dict(part=p.name,input_sha256=sha(p),input_rows=c.execute('SELECT count(*) FROM part').fetchone()[0])
  for name,pred in predicates.items():
   dest=out/name/p.name
   c.execute(f'COPY (SELECT p.* FROM part p JOIN (SELECT source_row FROM selected WHERE {pred}) s USING(source_row)) TO {q(dest)} (FORMAT PARQUET,COMPRESSION ZSTD)')
   k=c.execute(f'SELECT count(*) FROM read_parquet({q(dest)})').fetchone()[0];totals[name]+=k;chk[name+'_rows']=k;chk[name+'_sha256']=sha(dest)
  assert sum(chk[name+'_rows'] for name in totals)==chk['input_rows']
  union=' UNION ALL '.join(f'SELECT * FROM read_parquet({q(out/name/p.name)})' for name in totals)
  assert c.execute(f'SELECT count(*)=count(distinct source_row) FROM ({union})').fetchone()[0]
  # Exact comparison of all original fields keyed by the stable unique source row.
  assert c.execute(f'SELECT count(*) FROM part p FULL JOIN ({union}) o ON p.source_row=o.source_row WHERE p IS DISTINCT FROM o').fetchone()[0]==0
  chk['exact_all_field_identity_verified']=True;checks.append(chk);(out/'checks'/p.with_suffix('.json').name).write_text(json.dumps(chk,indent=2),encoding='utf-8')
  print('partition_complete',p.name,flush=True)
 assert totals['accepted']==counts[3] and totals['below_threshold']==counts[5] and totals['duplicate_removed']==n-counts[1]
 for name,pred in [('accepted','unique_points>=15'),('below_threshold','unique_points<15')]:
  c.execute(f'COPY (SELECT * FROM species_counts WHERE {pred} ORDER BY species_key) TO {q(out/(name+"_species_counts.csv"))} (HEADER)')
 c.execute(f"COPY (SELECT * FROM species_counts WHERE input_records>=15 AND unique_points<15 ORDER BY species_key) TO {q(out/'species_crossing_below15_due_to_dedup.csv')} (HEADER)")
 s=dict(completed=True,input_rows=n,input_species=counts[2],deduplicated_rows=counts[1],duplicate_removed_rows=totals['duplicate_removed'],accepted_rows=totals['accepted'],accepted_species=counts[4],below_threshold_rows=totals['below_threshold'],below_threshold_species=counts[6],species_crossing_below15=c.execute('SELECT count(*) FROM species_counts WHERE input_records>=15 AND unique_points<15').fetchone()[0],all_64_partitions_exact_fields_verified=True,observed_priority_verified=True,minimum_records=15,environment_and_spatial_filtering_not_done=True)
 for fn,data in [('summary.json',s),('file_checksums.json',checks)]: (out/fn).write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
 c.execute('CHECKPOINT');c.close();print(json.dumps(s),flush=True)
if __name__=='__main__':main()
