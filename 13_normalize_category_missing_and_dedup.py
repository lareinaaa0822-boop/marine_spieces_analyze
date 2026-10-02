"""Normalize only literal NA/empty in sex,lifestage, then globally deduplicate.

All other original non-ID fields must match EXACTLY as text. No numeric rounding,
scientific-name normalization or depth processing. Earliest source_row wins.
Hashes identify candidates; every duplicate group is verified on actual fields.
"""
import argparse,json,hashlib
from pathlib import Path
import duckdb

def q(s):return "'"+str(s).replace("'","''")+"'"
def qi(s):return '"'+s.replace('"','""')+'"'
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(8388608),b''):h.update(b)
 return h.hexdigest()
def main():
 p=argparse.ArgumentParser()
 for k in ['source','raw-manifest','out']:p.add_argument('--'+k,required=True)
 a=p.parse_args();src,out=Path(a.source),Path(a.out);out.mkdir(parents=True,exist_ok=False)
 for d in ['accepted','checks','audit']:(out/d).mkdir()
 keys=json.loads(Path(a.raw_manifest).read_text(encoding='utf-8'))['keys']
 c=duckdb.connect(str(out/'audit'/'dedup_index.duckdb'));c.execute("SET memory_limit='4GB'");c.execute('SET threads=4');c.execute(f"SET temp_directory={q(out/'audit'/'spill')}")
 def expr(k):return f"CASE WHEN {qi(k)} IS NULL OR {qi(k)} IN ('','NA') THEN NULL ELSE {qi(k)} END" if k in ['sex','lifestage'] else qi(k)
 sig='sha256(to_json(struct_pack('+','.join(qi(k)+' := '+expr(k) for k in keys)+')))'
 c.execute('CREATE TABLE signatures(source_row BIGINT,id VARCHAR,sig VARCHAR)')
 parts=sorted(src.glob('part_*.parquet'));assert len(parts)==64
 hashes=[]
 for part in parts:
  c.execute(f'INSERT INTO signatures SELECT source_row,id,{sig} FROM read_parquet({q(part)})')
  hashes.append(dict(partition=part.name,input_sha256=sha(part)))
  print('indexed',part.name,flush=True)
 c.execute('CREATE TABLE groups AS SELECT sig,min(source_row) retained_source_row,count(*) group_size FROM signatures GROUP BY sig HAVING count(*)>1')
 c.execute('CREATE TABLE members AS SELECT s.*,g.retained_source_row,g.group_size FROM signatures s JOIN groups g USING(sig)')
 c.execute('CREATE TABLE removed AS SELECT * FROM members WHERE source_row<>retained_source_row')
 # Verify every candidate group by full equality, never trust hashes alone.
 c.execute(f"CREATE TEMP TABLE candidate_rows AS SELECT r.*,m.sig,m.retained_source_row,m.group_size FROM read_parquet({q(src/'*.parquet')}) r JOIN members m USING(source_row)")
 diff=' OR '.join(f'({expr(k).replace(qi(k),"a."+qi(k))}) IS DISTINCT FROM ({expr(k).replace(qi(k),"b."+qi(k))})' for k in keys)
 failures=c.execute('SELECT count(*) FROM candidate_rows a JOIN candidate_rows b ON a.retained_source_row=b.source_row WHERE '+diff).fetchone()[0]
 assert failures==0,'Hash collision or unexpected field difference; refusing deletion'
 c.execute(f"COPY (SELECT a.source_row removed_source_row,a.id removed_id,a.retained_source_row,b.id retained_id,a.sex removed_sex_raw,b.sex retained_sex_raw,a.lifestage removed_lifestage_raw,b.lifestage retained_lifestage_raw,a.group_size FROM candidate_rows a JOIN candidate_rows b ON a.retained_source_row=b.source_row WHERE a.source_row<>a.retained_source_row ORDER BY a.source_row) TO {q(out/'removed_mapping.csv')} (HEADER)")
 c.execute(f"COPY (SELECT * EXCLUDE(sig,retained_source_row,group_size) FROM candidate_rows WHERE source_row<>retained_source_row) TO {q(out/'removed_records.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
 total,removed=c.execute('SELECT (SELECT count(*) FROM signatures),(SELECT count(*) FROM removed)').fetchone()
 accepted=0
 for part,entry in zip(parts,hashes):
  c.execute(f"COPY (SELECT r.* EXCLUDE(sex,lifestage),r.sex AS sex_raw,r.lifestage AS lifestage_raw,CASE WHEN r.sex IS NULL OR r.sex IN ('','NA') THEN NULL ELSE r.sex END AS sex,CASE WHEN r.lifestage IS NULL OR r.lifestage IN ('','NA') THEN NULL ELSE r.lifestage END AS lifestage,coalesce(m.group_size,1) AS category_duplicate_group_size FROM read_parquet({q(part)}) r LEFT JOIN members m USING(source_row) WHERE m.source_row IS NULL OR m.source_row=m.retained_source_row) TO {q(out/'accepted'/part.name)} (FORMAT PARQUET,COMPRESSION ZSTD)")
  n=c.execute(f'SELECT count(*) FROM read_parquet({q(out/"accepted"/part.name)})').fetchone()[0];accepted+=n
  entry.update(accepted_rows=n,output_sha256=sha(out/'accepted'/part.name))
  (out/'checks'/part.with_suffix('.json').name).write_text(json.dumps(entry),encoding='utf-8')
  print('written',part.name,flush=True)
 assert accepted+removed==total
 c.execute(f'CREATE VIEW output_rows AS SELECT * FROM read_parquet({q(out/"accepted"/"*.parquet")})')
 rows,species,bad=c.execute("SELECT count(*),count(distinct lower(trim(scientificname))),count(*) FILTER(WHERE sex IN ('','NA') OR lifestage IN ('','NA')) FROM output_rows").fetchone()
 assert rows==accepted and bad==0
 # Verify output identity against independently stored selection index.
 unexpected=c.execute('SELECT count(*) FROM ((SELECT source_row FROM output_rows EXCEPT SELECT source_row FROM signatures WHERE source_row NOT IN (SELECT source_row FROM removed)) UNION ALL (SELECT source_row FROM signatures WHERE source_row NOT IN (SELECT source_row FROM removed) EXCEPT SELECT source_row FROM output_rows))').fetchone()[0]
 assert unexpected==0
 c.execute(f"COPY (SELECT lower(trim(scientificname)) species_key,count(*) records FROM output_rows GROUP BY 1 ORDER BY 1) TO {q(out/'retained_species_counts.csv')} (HEADER)")
 s=dict(source=str(src),input_rows=total,removed_rows=removed,accepted_rows=accepted,retained_species=species,duplicate_groups=c.execute('SELECT count(*) FROM groups').fetchone()[0],all_duplicate_groups_verified_on_full_fields=True,output_source_rows_verified=True,rule="Only sex/lifestage: literal 'NA', empty and NULL -> NULL; every other original non-ID field exact text; earliest source_row retained",no_numeric_rounding=True,original_category_values_preserved=['sex_raw','lifestage_raw'],depth_year_environment_unchanged=True,completed=True)
 (out/'summary.json').write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding='utf-8');(out/'file_checksums.json').write_text(json.dumps(hashes,indent=2),encoding='utf-8')
 c.execute('CHECKPOINT');c.close();print(json.dumps(s,ensure_ascii=True),flush=True)
if __name__=='__main__':main()
