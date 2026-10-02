"""Read IUCN range attributes and compare species lists; do not filter records."""
from pathlib import Path
import json,hashlib
import pandas as pd
import pyogrio
root=Path('D:/BaiduNetdiskDownload/IUCN_marine1/IUCN_marine')
out=Path('D:/IUCN_marine/rebuild_20260930/iucn_range_species_audit_20261002');out.mkdir(exist_ok=True)
frames=[];inventory=[]
for f in sorted(root.rglob('*.shp')):
 if 'buffer' in f.stem.lower():continue
 fields=list(pyogrio.read_info(f)['fields']);names={x.lower():x for x in fields}
 key=next((names[n] for n in ['sci_name','binomial','scientificname'] if n in names),None)
 if not key:raise ValueError(f'No species name field: {f}')
 selected=[key]+[names[n] for n in ['presence','origin','marine'] if n in names]
 d=pyogrio.read_dataframe(f,columns=selected,read_geometry=False)
 d=d.rename(columns={v:k for k,v in names.items() if v in selected});d=d.rename(columns={key.lower():'scientificname'})
 d['species_key']=d.scientificname.fillna('').str.strip().str.lower();d['source_file']=str(f)
 d['presence_origin_123']=pd.to_numeric(d.get('presence'),errors='coerce').isin([1,2,3]) & pd.to_numeric(d.get('origin'),errors='coerce').isin([1,2,3])
 d['marine_true']=d.get('marine',pd.Series('',index=d.index)).astype(str).str.strip().str.lower().isin(['true','1'])
 frames.append(d);inventory.append(dict(file=str(f),features=len(d),species=d.loc[d.species_key!='','species_key'].nunique(),dbf_sha256=hashlib.sha256(f.with_suffix('.dbf').read_bytes()).hexdigest()))
 print(f.parent.name,len(d),flush=True)
allrows=pd.concat(frames,ignore_index=True);allrows.to_csv(out/'range_attribute_inventory.csv',index=False,encoding='utf-8-sig')
counts=allrows[allrows.species_key!=''].groupby('species_key').agg(range_rows=('species_key','size'),rows_presence_origin_123=('presence_origin_123','sum'),rows_marine=('marine_true','sum'))
qualified=allrows[allrows.presence_origin_123 & allrows.marine_true].groupby('species_key').size()
counts['rows_marine_presence_origin_123']=qualified.reindex(counts.index,fill_value=0);counts.reset_index().to_csv(out/'range_species_counts.csv',index=False,encoding='utf-8-sig')
current=pd.read_csv('G:/Marine_AOH_rebuild_20260930/08_species_minimum_15/accepted_species_counts.csv')
current=current.merge(counts.reset_index(),on='species_key',how='left');current['has_range']=current.range_rows.notna();current['has_marine_selected_range']=current.rows_marine_presence_origin_123.fillna(0)>0
current.to_csv(out/'current_7318_species_range_membership.csv',index=False,encoding='utf-8-sig')
current[~current.has_range].to_csv(out/'species_without_range.csv',index=False,encoding='utf-8-sig')
current[current.has_range & ~current.has_marine_selected_range].to_csv(out/'species_without_marine_selected_range.csv',index=False,encoding='utf-8-sig')
s=dict(source=str(root),shapefiles=len(inventory),range_features=len(allrows),unique_range_species=len(counts),marine_presence_origin_123_species=len(qualified),current_species=len(current),current_with_range=int(current.has_range.sum()),current_without_range=int((~current.has_range).sum()),current_with_marine_selected_range=int(current.has_marine_selected_range.sum()),records_without_range=int(current.loc[~current.has_range,'records'].sum()),production_records_unchanged=True,geometry_validity_not_checked=True)
(out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');(out/'source_inventory.json').write_text(json.dumps(inventory,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(s,ensure_ascii=False),flush=True)
