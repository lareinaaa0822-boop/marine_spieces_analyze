"""Compare fresh conservative raw dedup to E archives without applying new modelling rules.
Comparison uses SHA-256 of canonical source payload (excluding id); numeric text
such as 1999 and 1999.0 is equated for CSV round-trip comparison. Hash matching is
an audit fingerprint, not the dedup rule. IUCN matching here is comparison-only.
"""
import argparse,csv,json,time
from pathlib import Path
import duckdb,pandas as pd

def q(s):return "'"+str(s).replace("'","''")+"'"
def i(s):return '"'+s.replace('"','""')+'"'

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--raw',required=True);ap.add_argument('--out',required=True);ap.add_argument('--wait-for-partitions',action='store_true');a=ap.parse_args()
    raw,out=Path(a.raw),Path(a.out);out.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(str(out/'comparison.duckdb'));con.execute("SET memory_limit='4GB'");con.execute('SET threads=2')
    (out/'spill').mkdir(exist_ok=True);con.execute(f'SET temp_directory={q(out/"spill")}')
    con.execute("SET max_temp_directory_size='25GB'");con.execute('SET enable_progress_bar=false')
    keys=json.loads((raw/'manifest.json').read_text(encoding='utf-8'))['keys']
    textcols={'scientificname','lifestage','sex','behavior','eventDate','country','countryCode','locality','continent','waterBody','habitat'}
    ex=[]
    for c in keys:
        if c in textcols:
            e=f"nullif(trim({i(c)}),'')"
            if c=='scientificname':e=f'lower({e})'
        else:
            n=f'try_cast({i(c)} AS DOUBLE)';e=f'CASE WHEN isfinite({n}) THEN {n} ELSE NULL END'
        ex.append(f'{i(c)} := {e}')
    signature='sha256(to_json(struct_pack('+','.join(ex)+')))'
    sheets=pd.read_excel('C:/Users/lareina/Downloads/Marine_species_info.xlsx')
    sheets['species_key']=sheets.sci_name.astype(str).str.strip().str.lower()
    for c in ['depth_up','depth_low']:sheets[c]=pd.to_numeric(sheets[c],errors='coerce')
    good=sheets[sheets.depth_up.notna()&sheets.depth_low.notna()&(sheets.depth_up>=0)&(sheets.depth_low>=0)].copy()
    conflicts=good.groupby('species_key').agg(rows=('species_key','size'),up_values=('depth_up','nunique'),low_values=('depth_low','nunique'))
    conflicts[conflicts.rows>1].to_csv(out/'iucn_duplicate_names.csv',encoding='utf-8-sig')
    good['iucn_mid']=(good.depth_up+good.depth_low)/2
    lookup=good[['species_key','depth_up','depth_low','iucn_mid']].drop_duplicates('species_key',keep='last')
    con.register('lookup_df',lookup);con.execute('CREATE OR REPLACE TABLE iucn AS SELECT * FROM lookup_df')
    summary={'iucn_table_rows':len(sheets),'iucn_valid_rows':len(good),'iucn_valid_names':len(lookup),
             'iucn_reversed_limits':int((good.depth_up>good.depth_low).sum()),
             'iucn_duplicate_names':int((conflicts.rows>1).sum()),'fingerprint':'SHA256 of canonical source fields excluding id',
             'comparison_only_no_depth_filled_dataset_created':True}
    oldfiles={
        'E_old_dedup':'E:/因物种重复造成的修改8月/数据/建模数据集_带深度标记_去重.csv',
        'E_strict_verified':'E:/重新做081/01_建模数据集_带深度标记_严格去重_验证版.csv'}
    def save(): (out/'comparison_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    for name,f in oldfiles.items():
        marker=out/(name+'.json')
        if marker.exists():summary[name]=json.loads(marker.read_text(encoding='utf-8'));continue
        print('old_start',name,flush=True)
        con.execute(f"CREATE OR REPLACE VIEW src AS SELECT * FROM read_csv({q(f)},all_varchar=true,header=true,strict_mode=true)")
        stat=con.execute('''SELECT count(*) AS n_rows, count(distinct scientificname) species,
          count(*) FILTER(WHERE try_cast(depth AS DOUBLE)<0) negative_original_depth,
          count(*) FILTER(WHERE try_cast(depth AS DOUBLE)>11000) excessive_original_depth,
          count(*) FILTER(WHERE depth_source='observed' AND try_cast(depth AS DOUBLE)<0) negative_kept_observed,
          count(*) FILTER(WHERE depth_source='iucn_mid' AND abs(try_cast(depth_final AS DOUBLE)-(try_cast(iucn_depth_up AS DOUBLE)+try_cast(iucn_depth_low AS DOUBLE))/2)>1e-8) midpoint_disagrees,
          count(*) FILTER(WHERE depth_source='observed' AND abs(try_cast(depth_final AS DOUBLE)-try_cast(depth AS DOUBLE))>1e-8) observed_changed,
          count(*) FILTER(WHERE try_cast(depth_final AS DOUBLE) IS NULL) missing_final_depth,
          count(*) FILTER(WHERE try_cast(year AS DOUBLE) IS NULL) missing_year,
          count(*) FILTER(WHERE try_cast(year AS DOUBLE)>2025) future_year,
          count(*) FILTER(WHERE try_cast(year AS DOUBLE)<1993) pre1993_year,
          count(*) FILTER(WHERE try_cast(longitude AS DOUBLE) IS NULL OR try_cast(latitude AS DOUBLE) IS NULL) missing_coordinates
          FROM src''').fetchdf().iloc[0].to_dict()
        stat={k:int(v) for k,v in stat.items()}
        stat['depth_source_counts']=con.execute('SELECT depth_source,count(*) FROM src GROUP BY 1').fetchall()
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT {signature} AS sig, lower(trim(scientificname)) species_key, count(*) n FROM src GROUP BY 1,2")
        stat['unique_normalized_payloads']=con.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
        stat['normalized_payload_duplicates']=stat['n_rows']-stat['unique_normalized_payloads']
        con.execute(f"COPY (SELECT scientificname,depth,depth_final,depth_source,iucn_depth_up,iucn_depth_low FROM src WHERE try_cast(depth AS DOUBLE)<0 LIMIT 30) TO {q(out/(name+'_negative_depth_examples.csv'))} (HEADER)")
        marker.write_text(json.dumps(stat,ensure_ascii=False,indent=2),encoding='utf-8');summary[name]=stat;save()
        print('old_done',name,stat,flush=True)
    # Archive-to-archive comparison does not depend on new processing.
    summary['old_vs_strict']=con.execute('''SELECT count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NOT NULL) common,
         count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NULL) old_only,
         count(*) FILTER(WHERE a.sig IS NULL AND b.sig IS NOT NULL) strict_only
         FROM E_old_dedup a FULL OUTER JOIN E_strict_verified b USING(sig)''').fetchdf().iloc[0].to_dict()
    summary['old_vs_strict']={k:int(v) for k,v in summary['old_vs_strict'].items()};save()
    if not (raw/'summary.json').exists() and not a.wait_for_partitions:
        print('WAITING_FOR_RAW_DEDUP: rerun this script after raw completion',flush=True);return
    if not (out/'new_signatures_complete.json').exists():
        con.execute('CREATE OR REPLACE TABLE comparison_species AS SELECT species_key FROM iucn UNION SELECT species_key FROM E_old_dedup UNION SELECT species_key FROM E_strict_verified')
        parts_out=out/'new_signature_parts';parts_out.mkdir(exist_ok=True)
        stats=[]
        nparts=json.loads((raw/'manifest.json').read_text(encoding='utf-8'))['partitions']
        for b in range(nparts):
            part=raw/'deduplicated'/f'part_{b:03}.parquet'
            ready=raw/'checks'/f'part_{b:03}.json'
            while not ready.exists():time.sleep(5)
            sigpart=parts_out/part.name;statpart=parts_out/(part.stem+'.json')
            if statpart.exists():
                stats.append(json.loads(statpart.read_text()));continue
            con.execute(f"CREATE OR REPLACE VIEW src AS SELECT r.* FROM read_parquet({q(part)}) r WHERE lower(trim(r.scientificname)) IN (SELECT species_key FROM comparison_species)")
            con.execute(f'COPY (SELECT {signature} AS sig,lower(trim(scientificname)) AS species_key,count(*) AS n FROM src GROUP BY 1,2) TO {q(sigpart)} (FORMAT PARQUET,COMPRESSION ZSTD)')
            s=con.execute('''SELECT count(*) AS n_rows, sum(duplicate_group_size) original_rows,
                count(*) FILTER(WHERE try_cast(depth AS DOUBLE)<0) negative_depth,
                count(*) FILTER(WHERE try_cast(depth AS DOUBLE)>11000) excessive_depth,
                count(*) FILTER(WHERE try_cast(depth AS DOUBLE) IS NULL) missing_depth,
                count(*) FILTER(WHERE try_cast(year AS DOUBLE) IS NULL) missing_year,
                count(*) FILTER(WHERE try_cast(year AS DOUBLE)>2025) future_year,
                count(*) FILTER(WHERE try_cast(year AS DOUBLE)<1993) pre1993_year,
                count(*) FILTER(WHERE try_cast(longitude AS DOUBLE) IS NULL OR try_cast(latitude AS DOUBLE) IS NULL) missing_coordinates FROM src''').fetchdf().iloc[0].to_dict()
            sr={k:int(v) for k,v in s.items()};stats.append(sr);statpart.write_text(json.dumps(sr),encoding='utf-8');print('new_part',part.name,flush=True)
        con.execute(f'CREATE OR REPLACE TABLE new_sigs AS SELECT sig,species_key,sum(n)::BIGINT n FROM read_parquet({q(parts_out/"*.parquet")}) GROUP BY 1,2')
        ns={k:sum(s[k] for s in stats) for k in stats[0]}
        ns['unique_normalized_payloads']=con.execute('SELECT count(*) FROM new_sigs').fetchone()[0]
        ns['species']=con.execute('SELECT count(distinct species_key) FROM new_sigs').fetchone()[0]
        (out/'new_signatures_complete.json').write_text(json.dumps(ns,indent=2),encoding='utf-8')
    summary['new_comparison_domain']=json.loads((out/'new_signatures_complete.json').read_text())
    summary['comparison_domain_definition']='Union of species in both E archives and the downloaded IUCN table; no final depth, coordinate or year filtering; not a modelling dataset'
    for name in oldfiles:
        comp=con.execute(f'''SELECT count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NOT NULL) common,
         count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NULL) new_only,
         count(*) FILTER(WHERE a.sig IS NULL AND b.sig IS NOT NULL) archive_only
         FROM new_sigs a FULL OUTER JOIN {name} b USING(sig)''').fetchdf().iloc[0].to_dict()
        summary['new_vs_'+name]={k:int(v) for k,v in comp.items()}
        con.execute(f"COPY (SELECT coalesce(a.species_key,b.species_key) species, count(*) FILTER(WHERE a.sig IS NOT NULL AND b.sig IS NOT NULL) common,count(*) FILTER(WHERE b.sig IS NULL) new_only,count(*) FILTER(WHERE a.sig IS NULL) archive_only FROM new_sigs a FULL OUTER JOIN {name} b USING(sig) GROUP BY 1 ORDER BY 1) TO {q(out/('species_new_vs_'+name+'.csv'))} (HEADER)")
        save()
    summary['completed']=True;save();con.close();print('comparison_complete',flush=True)

if __name__=='__main__':main()

