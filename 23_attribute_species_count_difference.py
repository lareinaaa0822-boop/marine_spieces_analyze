"""Attribute species-list differences using saved counts; no filtering."""
from pathlib import Path
import pandas as pd,json
base=Path('D:/IUCN_marine/rebuild_20260930/historical_species_filters_20261002')
out=Path('D:/IUCN_marine/rebuild_20260930/species_count_difference_20261002');out.mkdir(exist_ok=True)
d=pd.read_csv(base/'before_point_key_dedup_species_comparison.csv').rename(columns={'records':'current_records','historical_records':'old_pre_point_dedup'})
for stage in ['after_point_key_dedup','before_environment','after_environment','after_buffer']:
 t=pd.read_csv(base/(stage+'_species_comparison.csv'))[['species_key','historical_records']].rename(columns={'historical_records':'old_'+stage});d=d.merge(t,on='species_key',how='left')
t=pd.read_csv(base/'old_presence_species_counts.csv').rename(columns={'records':'old_saved_presence_records'});d=d.merge(t,on='species_key',how='left')
t=pd.read_csv(base/'current_record_key_dedup_species_counts.csv').rename(columns={'records':'current_distinct_point_keys'});d=d.merge(t,on='species_key',how='left')
d['difference_group']='shared_old_model';d.loc[~d.old_model & d.old_presence,'difference_group']='old_presence_without_model';d.loc[~d.old_presence,'difference_group']='absent_old_saved_presence'
d['first_old_stage_below15']='never_below15_in_examined_old_stages'
for column,label in [('old_pre_point_dedup','before_point_dedup'),('old_after_point_key_dedup','point_key_dedup'),('old_before_environment','year_and_depth_filter'),('old_after_environment','environment_filter'),('old_after_buffer','buffer_filter')]:
 mask=(d.first_old_stage_below15=='never_below15_in_examined_old_stages') & (d[column].fillna(0)<15)
 d.loc[mask,'first_old_stage_below15']=label
d.to_csv(out/'all_current_species_difference_attribution.csv',index=False,encoding='utf-8-sig')
extra=d[~d.old_presence];extra.to_csv(out/'1924_species_absent_old_presence.csv',index=False,encoding='utf-8-sig')
extra[extra.first_old_stage_below15=='never_below15_in_examined_old_stages'].to_csv(out/'unexplained_by_examined_old_filters.csv',index=False,encoding='utf-8-sig')
d[~d.old_model & d.old_presence].to_csv(out/'old_presence_without_model.csv',index=False,encoding='utf-8-sig')
d[d.current_distinct_point_keys<15].to_csv(out/'current_species_below15_after_point_key_dedup_diagnostic.csv',index=False,encoding='utf-8-sig')
old={f.stem.replace('_',' ').strip().lower() for f in Path('D:/Maxent_rds').glob('*.rds')}
missing=pd.DataFrame({'species_key':sorted(old-set(d.species_key))})
for label,path in [('after_category_dedup','G:/Marine_AOH_rebuild_20260930/05_category_missing_dedup/retained_species_counts.csv'),('after_depth','G:/Marine_AOH_rebuild_20260930/06_depth_assignment/accepted_species_counts.csv'),('after_year','G:/Marine_AOH_rebuild_20260930/07_year_quality/accepted_species_counts.csv')]:
 t=pd.read_csv(path);print(label,t.columns.tolist(),flush=True)
 t=t.rename(columns={'records':label+'_records'});missing=missing.merge(t[['species_key',label+'_records']],on='species_key',how='left')
missing['reason']='under15_after_current_year_filter'
missing.loc[missing.after_year_records.fillna(0)==0,'reason']='no_current_in_period_records'
missing.loc[missing.after_depth_records.fillna(0)==0,'reason']='no_current_depth_accepted_records'
missing.loc[missing.after_category_dedup_records.fillna(0)==0,'reason']='absent_before_depth_stage'
missing.to_csv(out/'274_old_models_not_in_current_initial_filter.csv',index=False,encoding='utf-8-sig')
s=dict(current_initial_species=len(d),old_models=len(old),shared=int(d.old_model.sum()),current_not_old_model=int((~d.old_model).sum()),old_not_current=len(missing),net_difference=len(d)-len(old),additional_species_without_old_presence=len(extra),first_threshold_loss_in_examined_E_chain=extra.first_old_stage_below15.value_counts().to_dict(),old_presence_without_model=len(d[~d.old_model & d.old_presence]),old_models_removed_current_reasons=missing.reason.value_counts().to_dict(),current_species_lost_after_point_key_dedup_diagnostic=int((d.current_distinct_point_keys<15).sum()),remaining_9_species=extra.loc[extra.first_old_stage_below15=='never_below15_in_examined_old_stages','species_key'].tolist(),limitations=['Historical E chain and saved G presence are different versions; first threshold loss describes that E chain only','No production filtering performed','Model input existence alone does not identify why training failed or was omitted'])
assert s['net_difference']==s['current_not_old_model']-s['old_not_current']
(out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False),flush=True)
