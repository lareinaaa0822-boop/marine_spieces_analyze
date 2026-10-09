from pathlib import Path
import duckdb,pandas as pd,json
import sys
if '--current-buffer-preflight' in sys.argv:
 import shapefile,shapely,numpy as np,hashlib
 from shapely.geometry import shape
 from pyproj import CRS
 audit=Path('D:/IUCN_marine/rebuild_20260930/spatial_preflight_20261003');audit.mkdir(exist_ok=True)
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 source='G:/Marine_AOH_rebuild_20260930/10_environment_first_min15/accepted/*.parquet'
 pts=c.execute(f"SELECT source_row,lower(trim(scientificname)) species_key,try_cast(longitude AS DOUBLE) longitude,try_cast(latitude AS DOUBLE) latitude,environment_outside_mask FROM read_parquet('{source}') ORDER BY species_key,source_row").fetchdf()
 assert len(pts)==5800207 and pts.source_row.is_unique
 wanted=set(pts.species_key);geoms={};inventory=[];issues=[]
 for f in sorted(Path('D:/IUCN_marine').glob('*/*_buffer.shp')):
  assert CRS.from_wkt(f.with_suffix('.prj').read_text()).equals(CRS.from_epsg(4326),ignore_axis_order=True)
  selected=0
  with shapefile.Reader(str(f)) as reader:
   for record in reader.iterShapeRecords():
    key=str(record.record.as_dict()['BINOMIAL']).strip().lower()
    if key not in wanted:continue
    geom=shape(record.shape.__geo_interface__);selected+=1
    if not geom.is_valid or geom.is_empty:issues.append(dict(species_key=key,path=str(f),invalid_features=1))
    geoms.setdefault(key,[]).append(geom)
  for side in sorted(f.parent.glob(f.stem+'.*')):
   inventory.append(dict(path=str(side),bytes=side.stat().st_size,sha256=hashlib.sha256(side.read_bytes()).hexdigest()))
  print('buffer_loaded',f.parent.name,selected,flush=True)
 issue_species={x['species_key'] for x in issues};findings=[];decisions=[]
 for i,(key,p) in enumerate(pts.groupby('species_key',sort=False)):
  status='ready';inside=np.zeros(len(p),dtype=bool);boundary=inside.copy()
  if key not in geoms:status='missing_buffer'
  elif key in issue_species:status='invalid_geometry'
  else:
   try:
    geom=shapely.union_all(geoms[key]);shapely.prepare(geom)
    points=shapely.points(p.longitude.to_numpy(),p.latitude.to_numpy());inside=shapely.contains(geom,points);boundary=shapely.intersects(geom,points)&~inside
   except Exception as e:status='geometry_error';issues.append(dict(species_key=key,error=str(e)))
  n=int(inside.sum());findings.append(dict(species_key=key,status=status,input_rows=len(p),inside_rows=n,boundary_rows=int(boundary.sum()),outside_rows=int((~inside&~boundary).sum()) if status=='ready' else None,retained_ge15=status=='ready' and n>=15,environment_outside_rows=int((p.environment_outside_mask!=0).sum())))
  decisions.append(pd.DataFrame(dict(source_row=p.source_row.to_numpy(),species_key=key,buffer_status=status,buffer_inside=inside,buffer_boundary=boundary)))
  if i%500==0:print('species_checked',i+1,flush=True)
 counts=pd.DataFrame(findings);counts.to_csv(audit/'current_buffer_species_counts.csv',index=False,encoding='utf-8-sig')
 old_inputs={f.stem.replace('_',' ').lower():f for f in Path('D:/buffer筛选版本').glob('*.csv')}
 old_outputs={f.stem.replace('_',' ').lower():f for f in Path('D:/过滤后的物种329').glob('*.csv')}
 oldchecks=[]
 for key in counts.loc[counts.boundary_rows>0,'species_key']:
  if key not in old_inputs:continue
  a=pd.read_csv(old_inputs[key]);actual=pd.read_csv(old_outputs[key]) if key in old_outputs else a.iloc[:0]
  geom=shapely.union_all(geoms[key]);points=shapely.points(a.longitude.to_numpy(),a.latitude.to_numpy())
  for mode,mask in [('contains',shapely.contains(geom,points)),('intersects',shapely.intersects(geom,points))]:
   predicted=a.loc[mask];predicted=predicted if len(predicted)>=15 else a.iloc[:0]
   oldchecks.append(dict(species_key=key,mode=mode,predicted_rows=len(predicted),actual_rows=len(actual),exact_records_equal=predicted.reset_index(drop=True).equals(actual.reset_index(drop=True))))
 pd.DataFrame(oldchecks).to_csv(audit/'boundary_species_historical_check.csv',index=False,encoding='utf-8-sig')
 missing=counts[counts.status=='missing_buffer'].copy();missing['in_old_spatial_output']=missing.species_key.isin(old_outputs);missing.to_csv(audit/'missing_buffer_species.csv',index=False,encoding='utf-8-sig')
 pd.concat(decisions,ignore_index=True).to_parquet(audit/'current_buffer_decisions.parquet',index=False,compression='zstd')
 pd.DataFrame(inventory).to_csv(audit/'buffer_file_inventory.csv',index=False,encoding='utf-8-sig')
 s=dict(completed=True,source=source,input_rows=len(pts),input_species=len(counts),status_counts=counts.status.value_counts().to_dict(),boundary_rows=int(counts.boundary_rows.sum()),inside_rows=int(counts.inside_rows.sum()),predicted_species_ge15=int(counts.retained_ge15.sum()),predicted_rows_ge15=int(counts.loc[counts.retained_ge15,'inside_rows'].sum()),current_environment_outside_rows=int((pts.environment_outside_mask!=0).sum()),issues=issues,production_data_changed=False)
 (audit/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False),flush=True);sys.exit(0)
if '--audit-first-environment-filter' in sys.argv:
 audit=Path('D:/IUCN_marine/rebuild_20260930/first_version_order_audit_20261002');audit.mkdir(exist_ok=True)
 db=duckdb.connect();db.execute("SET memory_limit='4GB'");db.execute('SET threads=4')
 db.execute("CREATE TABLE counts AS SELECT lower(trim(scientificname)) species_key,count(*) records FROM read_csv('D:/取环境值4.csv',all_varchar=true) WHERE try_cast(thetao AS DOUBLE)<>-1 AND try_cast(so AS DOUBLE)<>-1 AND try_cast(chl AS DOUBLE)<>-1 AND try_cast(no3 AS DOUBLE)<>-1 AND try_cast(o2 AS DOUBLE)<>-1 GROUP BY 1")
 d=db.execute('SELECT * FROM counts').fetchdf();d.to_csv(audit/'D_environment_valid_species_counts.csv',index=False,encoding='utf-8-sig')
 b=pd.read_csv(audit/'D_buffer_species_counts.csv').dropna(subset=['species_key']);m=d.merge(b,on='species_key',how='outer',suffixes=('_env_valid','_buffer'),indicator=True)
 m.to_csv(audit/'environment_valid_to_buffer_comparison.csv',index=False,encoding='utf-8-sig')
 print(json.dumps(dict(valid_rows=int(d.records.sum()),valid_species=len(d),valid_species_ge15=int((d.records>=15).sum()),valid_rows_ge15=int(d.loc[d.records>=15,'records'].sum()),shared_count_changes=int(((m['_merge']=='both')&(m.records_env_valid!=m.records_buffer)).sum()),stage_membership=m['_merge'].value_counts().to_dict())),flush=True);sys.exit(0)
if '--verify-buffer-transition' in sys.argv:
 import pyogrio,shapely
 audit=Path('D:/IUCN_marine/rebuild_20260930/first_version_order_audit_20261002');audit.mkdir(exist_ok=True)
 species=pd.read_csv('D:/IUCN_marine/rebuild_20260930/stored_environment_versions_20261002/training_input_trace.csv').species.tolist()
 species+=['Abudefduf_conformis','Antennarius_pictus']
 wanted={sp.replace('_',' ').lower():sp for sp in species};geoms={}
 for f in sorted(Path('D:/IUCN_marine').glob('*/*_buffer.shp')):
  attrs=pyogrio.read_dataframe(f,columns=['BINOMIAL'],read_geometry=False)
  selected=attrs.loc[attrs.BINOMIAL.str.lower().isin(wanted),'BINOMIAL'].dropna().unique()
  if not len(selected):continue
  quoted=','.join("'"+n.replace("'","''")+"'" for n in selected)
  g=pyogrio.read_dataframe(f,where=f'BINOMIAL IN ({quoted})')
  for name,part in g.groupby('BINOMIAL'):geoms.setdefault(name.lower(),[]).extend(part.geometry.tolist())
 findings=[]
 for key,sp in wanted.items():
  a_path=Path('D:/buffer筛选版本')/(sp+'.csv');b_path=Path('D:/过滤后的物种329')/(sp+'.csv')
  if not a_path.exists() or key not in geoms:continue
  a=pd.read_csv(a_path);geom=shapely.union_all(geoms[key]);points=shapely.points(a.longitude.to_numpy(),a.latitude.to_numpy())
  for mode,mask in [('contains',shapely.contains(geom,points)),('intersects',shapely.intersects(geom,points))]:
   predicted=a[mask];predicted=predicted if len(predicted)>=15 else predicted.iloc[0:0]
   actual=pd.read_csv(b_path) if b_path.exists() else a.iloc[0:0]
   equal=predicted.reset_index(drop=True).equals(actual.reset_index(drop=True))
   findings.append(dict(species=sp,mode=mode,input_rows=len(a),predicted_rows=len(predicted),actual_rows=len(actual),exact_records_equal=equal))
  print(sp,findings[-2:],flush=True)
 pd.DataFrame(findings).to_csv(audit/'buffer_to_329_spatial_sample_verification.csv',index=False,encoding='utf-8-sig');sys.exit(0)
if '--audit-first-version' in sys.argv:
 audit=Path('D:/IUCN_marine/rebuild_20260930/first_version_order_audit_20261002');audit.mkdir(exist_ok=True)
 db=duckdb.connect();db.execute("SET memory_limit='6GB'");db.execute('SET threads=4')
 stages=[('D_before_year','D:/交集带年份未筛选.csv'),('D_after_year','D:/带年份筛选15.csv'),('D_environment','D:/取环境值4.csv'),('D_buffer','D:/buffer筛选版本/*.csv'),('D_min15','D:/过滤后的物种329/*.csv')]
 selected_stage=sys.argv[sys.argv.index('--stage')+1] if '--stage' in sys.argv else None
 findings=json.loads((audit/'stage_summary.json').read_text(encoding='utf-8')) if selected_stage and (audit/'stage_summary.json').exists() else []
 for label,path in stages:
  if selected_stage and label!=selected_stage:continue
  escaped=path.replace("'","''")
  db.execute(f"CREATE OR REPLACE TEMP TABLE stage AS SELECT * FROM read_csv('{escaped}',all_varchar=true,union_by_name=true) WHERE scientificname IS NOT NULL AND trim(scientificname)<>''")
  columns=[r[0] for r in db.execute('DESCRIBE stage').fetchall()]
  stats=db.execute("SELECT count(*),count(distinct lower(trim(scientificname))),count(*) FILTER(WHERE try_cast(year AS DOUBLE)<1993),count(*) FILTER(WHERE try_cast(year AS DOUBLE)>2025),count(*) FILTER(WHERE try_cast(year AS DOUBLE) IS NULL),count(*) FILTER(WHERE try_cast(depth AS DOUBLE)<0),count(*) FILTER(WHERE try_cast(depth_flag AS INTEGER)=0) FROM stage").fetchone()
  db.execute('CREATE OR REPLACE TEMP TABLE counts AS SELECT lower(trim(scientificname)) species_key,count(*) records FROM stage GROUP BY 1')
  target=str(audit/(label+'_species_counts.csv')).replace("'","''")
  db.execute(f"COPY (SELECT * FROM counts ORDER BY 1) TO '{target}' (HEADER)")
  r=dict(stage=label,path=path,**dict(zip(['rows','species','year_before1993','year_after2025','year_missing','negative_depth','observed_rows'],stats)),species_ge15=db.execute('SELECT count(*) FROM counts WHERE records>=15').fetchone()[0])
  if 'thetao' in columns:
   r['environment_invalid']=dict(zip(['any_minus1','any_null','thetao_zero','so_zero'],db.execute("SELECT count(*) FILTER(WHERE try_cast(thetao AS DOUBLE)=-1 OR try_cast(so AS DOUBLE)=-1 OR try_cast(chl AS DOUBLE)=-1 OR try_cast(no3 AS DOUBLE)=-1 OR try_cast(o2 AS DOUBLE)=-1),count(*) FILTER(WHERE try_cast(thetao AS DOUBLE) IS NULL OR try_cast(so AS DOUBLE) IS NULL OR try_cast(chl AS DOUBLE) IS NULL OR try_cast(no3 AS DOUBLE) IS NULL OR try_cast(o2 AS DOUBLE) IS NULL),count(*) FILTER(WHERE try_cast(thetao AS DOUBLE)=0),count(*) FILTER(WHERE try_cast(so AS DOUBLE)=0) FROM stage").fetchone()))
  findings=[v for v in findings if v['stage']!=label];findings.append(r);(audit/'stage_summary.json').write_text(json.dumps(findings,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(r),flush=True)
 print('first_version_stage_audit_complete',flush=True);sys.exit(0)
out=Path('D:/IUCN_marine/rebuild_20260930/species_count_difference_20261002')
c=duckdb.connect();c.execute("SET memory_limit='2GB'");c.execute('SET threads=2')
c.execute("CREATE TEMP TABLE prior AS SELECT lower(trim(scientificname)) species_key,count(*) records,count(*) FILTER(WHERE try_cast(depth_flag AS INTEGER)=0) observed_records,count(*) FILTER(WHERE try_cast(depth_flag AS INTEGER)=1) filled_records FROM read_csv('D:/过滤后的物种329/*.csv',all_varchar=true,union_by_name=true) GROUP BY 1")
c.execute("COPY (SELECT *,CASE WHEN observed_records>=10 THEN observed_records ELSE records END predicted_processed_records FROM prior ORDER BY 1) TO 'D:/IUCN_marine/rebuild_20260930/species_count_difference_20261002/D_prior_presence_species_counts.csv' (HEADER)")
d=pd.read_csv(out/'D_prior_presence_species_counts.csv');g=pd.read_csv('D:/IUCN_marine/rebuild_20260930/historical_species_filters_20261002/old_presence_species_counts.csv').rename(columns={'records':'saved_G_records'})
joined=d.merge(g,on='species_key',how='outer',indicator=True);joined['count_matches_observed_priority_rule']=joined.predicted_processed_records==joined.saved_G_records
joined.to_csv(out/'actual_prior_vs_saved_presence.csv',index=False,encoding='utf-8-sig')
cur=pd.read_csv(out/'all_current_species_difference_attribution.csv');cur=cur.merge(d,on='species_key',how='left',suffixes=('','_Dprior'));cur.to_csv(out/'current_species_vs_actual_D_prior.csv',index=False,encoding='utf-8-sig')
extra=cur[~cur.old_presence].copy();extra['D_prior_status']='present_but_missing_final_G'
extra.loc[extra.records.isna(),'D_prior_status']='absent_actual_D_prior'
extra.loc[extra.records.notna() & (extra.records<10),'D_prior_status']='under10_in_actual_D_prior'
extra.to_csv(out/'1924_species_actual_D_prior_trace.csv',index=False,encoding='utf-8-sig')
s=dict(actual_D_prior_species=len(d),actual_D_prior_rows=int(d.records.sum()),saved_G_species=len(g),join_counts=joined['_merge'].value_counts().to_dict(),matching_priority_rule_counts=int(joined.count_matches_observed_priority_rule.sum()),mismatching_common_counts=int(((joined['_merge']=='both') & ~joined.count_matches_observed_priority_rule).sum()),priority_rule='if observed depth_flag0 >=10, retain observed only; otherwise retain all',additional_species_status=extra.D_prior_status.value_counts().to_dict(),D_prior_minimum_records=int(d.records.min()),actual_D_prior_ge10=int((d.records>=10).sum()),actual_D_prior_ge15=int((d.records>=15).sum()))
checks=[]
for sp in pd.read_csv('D:/IUCN_marine/rebuild_20260930/stored_environment_versions_20261002/training_input_trace.csv').species:
 a=pd.read_csv(Path('D:/过滤后的物种329')/(sp+'.csv'));b=pd.read_csv(Path('G:/物种样本点处理后')/(sp+'.csv'));n=int((a.depth_flag==0).sum());a=a[a.depth_flag==0] if n>=10 else a
 checks.append(dict(species=sp,expected_rows=len(a),saved_rows=len(b),all_fields_sequence_equal=a.reset_index(drop=True).equals(b.reset_index(drop=True))))
pd.DataFrame(checks).to_csv(out/'observed_priority_rule_27_species_full_record_check.csv',index=False,encoding='utf-8-sig')
s['full_record_sample_checks']=checks
(out/'actual_final_presence_trace_summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False),flush=True)
