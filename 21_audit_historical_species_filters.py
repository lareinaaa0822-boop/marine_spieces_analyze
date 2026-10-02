"""Read-only stage count audit; no historical criteria applied to new data."""
from pathlib import Path
import json,duckdb
out=Path('D:/IUCN_marine/rebuild_20260930/historical_species_filters_20261002');out.mkdir(exist_ok=True)
c=duckdb.connect();c.execute("SET memory_limit='2GB'");c.execute('SET threads=2')
current=Path('G:/Marine_AOH_rebuild_20260930/08_species_minimum_15/accepted_species_counts.csv')
c.execute('CREATE TEMP TABLE current AS SELECT * FROM read_csv(?,all_varchar=true)',[str(current)])
oldmodels={f.stem.replace('_',' ').lower().strip() for f in Path('D:/Maxent_rds').glob('*.rds')}
oldpresence={f.stem.replace('_',' ').lower().strip() for f in Path('G:/物种样本点处理后').glob('*.csv')}
c.execute('ALTER TABLE current ADD COLUMN old_model BOOLEAN');c.execute('ALTER TABLE current ADD COLUMN old_presence BOOLEAN')
c.executemany('UPDATE current SET old_model=?,old_presence=? WHERE species_key=?',[(k in oldmodels,k in oldpresence,k) for (k,) in c.execute('SELECT species_key FROM current').fetchall()])
counts=[]
stages=[('before_point_key_dedup','带年份筛选15.csv'),('after_point_key_dedup','按物种坐标深度年份去重.csv'),('before_environment','取环境值_年份筛选后.csv'),('after_environment','取环境值4.csv'),('after_buffer','buffer筛选版本.csv')]
for stage,fn in stages:
 c.execute('CREATE OR REPLACE TEMP TABLE old_counts AS SELECT lower(trim(species)) species_key,count(*) records FROM read_csv(?,all_varchar=true) GROUP BY 1',['E:/因物种重复造成的修改8月/数据/'+fn])
 overall=c.execute('SELECT sum(records),count(*),count(*) FILTER(WHERE records>=15),sum(records) FILTER(WHERE records>=15) FROM old_counts').fetchone()
 missing=c.execute('SELECT count(*),count(*) FILTER(WHERE o.species_key IS NOT NULL),count(*) FILTER(WHERE o.records>=15),count(*) FILTER(WHERE o.records<15),count(*) FILTER(WHERE o.species_key IS NULL) FROM current n LEFT JOIN old_counts o USING(species_key) WHERE NOT n.old_presence').fetchone()
 r=dict(stage=stage,file=fn,**dict(zip(['rows','species','species_ge15','rows_in_species_ge15'],overall)),additional_current_species=dict(zip(['total','found','ge15','under15','absent'],missing)));counts.append(r)
 path=str(out/(stage+'_species_comparison.csv')).replace("'","''")
 c.execute(f"COPY (SELECT n.*,o.records historical_records FROM current n LEFT JOIN old_counts o USING(species_key) ORDER BY 1) TO '{path}' (HEADER)")
 print(json.dumps(r),flush=True)
path=str(out/'current_record_key_dedup_species_counts.csv').replace("'","''")
c.execute("CREATE TEMP TABLE distinct_points AS SELECT lower(trim(scientificname)) species_key,longitude,latitude,depth_final,year_final FROM read_parquet('G:/Marine_AOH_rebuild_20260930/07_year_quality/accepted/part_*.parquet') GROUP BY ALL")
c.execute('CREATE TEMP TABLE key_counts AS SELECT species_key,count(*) records FROM distinct_points GROUP BY 1')
c.execute(f"COPY (SELECT * FROM key_counts ORDER BY 1) TO '{path}' (HEADER)")
diagnostic=c.execute('SELECT sum(records),count(*) FILTER(WHERE records>=15),sum(records) FILTER(WHERE records>=15) FROM key_counts').fetchone()
result=dict(stages=counts,current_record_key_dedup_diagnostic=dict(zip(['distinct_point_keys','species_ge15','rows_in_species_ge15'],diagnostic)),diagnostic_only=True,numeric_depth_final_used_for_diagnostic=True,production_data_unchanged=True)
for label,pattern in [('old_presence','G:/物种样本点处理后/*.csv'),('old_buffer','E:/因物种重复造成的修改8月/数据/buffer筛选版本.csv')]:
 column='scientificname' if label=='old_presence' else 'species'
 c.execute(f'CREATE OR REPLACE TEMP TABLE saved_counts AS SELECT lower(trim({column})) species_key,count(*) records FROM read_csv(?,all_varchar=true,union_by_name=true) GROUP BY 1',[pattern])
 stats=c.execute('SELECT sum(records),count(*),min(records),count(*) FILTER(WHERE records>=10),count(*) FILTER(WHERE records>=15),count(*) FILTER(WHERE records BETWEEN 10 AND 14) FROM saved_counts').fetchone()
 result[label+'_threshold_counts']=dict(zip(['rows','species','minimum','species_ge10','species_ge15','species_10_to14'],stats))
 target=str(out/(label+'_species_counts.csv')).replace("'","''")
 c.execute(f"COPY (SELECT * FROM saved_counts ORDER BY 1) TO '{target}' (HEADER)")
 print(label,json.dumps(result[label+'_threshold_counts']),flush=True)
(out/'summary.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result),flush=True)
