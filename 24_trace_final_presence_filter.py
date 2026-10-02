from pathlib import Path
import duckdb,pandas as pd,json
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
