"""Audit E point-row alignment and depth/year/flag rules; never changes E data."""
import argparse,json
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"

def main():
 p=argparse.ArgumentParser();p.add_argument('--out',required=True);a=p.parse_args();out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 c=duckdb.connect();c.execute("SET memory_limit='2GB'");c.execute('SET threads=1');c.execute('SET preserve_insertion_order=true')
 c.execute(f'SET temp_directory={q(out/"spill_alignment")}')
 src='E:/因物种重复造成的修改8月/数据/取环境值_年份筛选后.csv'
 c.execute(f"CREATE TEMP TABLE pts AS SELECT row_number() OVER ()-1 row_id, species,try_cast(lon AS DOUBLE) lon,try_cast(lat AS DOUBLE) lat,try_cast(depth AS DOUBLE) depth,try_cast(year AS DOUBLE) AS year,depth_source,depth_flag FROM read_csv({q(src)},all_varchar=true,parallel=false)")
 print('loaded_old_points',flush=True)
 outdata={}
 query="""SELECT count(*) AS compared_rows,
 count(*) FILTER(WHERE a.row_id IS NULL) missing_source,
 count(*) FILTER(WHERE b.row_id IS NULL) missing_cache,
 count(*) FILTER(WHERE a.lon IS DISTINCT FROM b.lon OR a.lat IS DISTINCT FROM b.lat OR a.depth IS DISTINCT FROM b.depth OR a.year IS DISTINCT FROM b.year) coordinate_depth_year_disagreement
 FROM pts a FULL OUTER JOIN read_parquet('D:/points_for_extract.parquet') b USING(row_id)"""
 outdata['point_cache_alignment']={k:int(v) for k,v in c.execute(query).fetchdf().iloc[0].to_dict().items()}
 outdata['flags']=c.execute('SELECT depth_source,depth_flag,count(*) FROM pts GROUP BY 1,2').fetchall()
 outdata['negative_depth_counts']=c.execute('SELECT depth,count(*) FROM pts WHERE depth<0 GROUP BY 1 ORDER BY 1').fetchall()
 outdata['negative_depth_sources']=c.execute('SELECT depth_source,depth_flag,depth,count(*) FROM pts WHERE depth<0 GROUP BY 1,2,3 ORDER BY 1,3').fetchall()
 outdata['environment_cache_counts']=c.execute("SELECT count(*),count(distinct row_id),count(*) FILTER(WHERE thetao=-1 OR so=-1 OR chl=-1 OR no3=-1 OR o2=-1),count(*) FILTER(WHERE thetao=0 OR so=0 OR chl=0 OR no3=0 OR o2=0) FROM read_parquet('D:/env_values_extracted.parquet')").fetchone()
 (out/'old_alignment_summary.json').write_text(json.dumps(outdata,ensure_ascii=False,indent=2),encoding='utf-8')
 f='E:/因物种重复造成的修改8月/数据/建模数据集_带深度标记_去重.csv'
 r=c.execute("SELECT count(*) FILTER(WHERE depth_source='iucn_mid' AND (try_cast(iucn_depth_up AS DOUBLE)<0 OR try_cast(iucn_depth_low AS DOUBLE)<0)) invalid_endpoint_fills,count(*) FILTER(WHERE depth_source='iucn_mid' AND try_cast(depth_final AS DOUBLE)<0) negative_midpoint_fills,count(*) FILTER(WHERE depth_source='iucn_mid' AND try_cast(depth_final AS DOUBLE)>=0 AND (try_cast(iucn_depth_up AS DOUBLE)<0 OR try_cast(iucn_depth_low AS DOUBLE)<0)) positive_but_invalid_endpoint_fills FROM read_csv(?,all_varchar=true)",[f]).fetchdf().iloc[0].to_dict()
 (out/'invalid_midpoint_counts.json').write_text(json.dumps({k:int(v) for k,v in r.items()},indent=2),encoding='utf-8')
 stages=[]
 for name in ['按物种坐标深度年份去重.csv','取环境值_年份筛选后.csv','取环境值4.csv','buffer筛选版本.csv']:
  f=Path('E:/因物种重复造成的修改8月/数据')/name
  row=c.execute("SELECT count(*) n_rows,count(distinct species) n_species,count(*) FILTER(WHERE try_cast(depth AS DOUBLE)<0) negative_depth,count(*) FILTER(WHERE try_cast(year AS DOUBLE)>2025 OR try_cast(year AS DOUBLE)<1993) outside_years FROM read_csv(?,all_varchar=true)",[str(f)]).fetchdf().iloc[0].to_dict()
  row={k:int(v) for k,v in row.items()};row['file']=str(f);stages.append(row)
 (out/'e_downstream_stage_counts.json').write_text(json.dumps(stages,ensure_ascii=False,indent=2),encoding='utf-8')
 print(json.dumps(outdata,ensure_ascii=False),flush=True)

if __name__=='__main__':main()

