"""Independent verification of saved Parquet counts and sampled duplicate mappings."""
import argparse,json
from pathlib import Path
import duckdb
import pyarrow.parquet as pq

def q(s):return "'"+str(s).replace("'","''")+"'"
def i(s):return '"'+s.replace('"','""')+'"'

def main():
 p=argparse.ArgumentParser();p.add_argument('--raw',required=True);p.add_argument('--out',required=True);a=p.parse_args()
 root=Path(a.raw);out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
 summary=json.loads((root/'summary.json').read_text());manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
 assert summary['completed']
 keep=list((root/'deduplicated').glob('*.parquet'));drop=list((root/'removed_mapping').glob('*.parquet'))
 assert len(keep)==len(drop)==manifest['partitions']
 nk=sum(pq.read_metadata(f).num_rows for f in keep);nd=sum(pq.read_metadata(f).num_rows for f in drop)
 assert nk==summary['retained_rows'] and nd==summary['removed_rows'] and nk+nd==summary['raw_rows']
 c=duckdb.connect();c.execute("SET memory_limit='4GB'");c.execute('SET threads=4')
 weighted=c.execute(f'SELECT sum(duplicate_group_size), min(duplicate_group_size),min(source_row),max(source_row) FROM read_parquet({q(root/"deduplicated"/"*.parquet")})').fetchone()
 assert weighted[0]==summary['raw_rows'] and weighted[1]>=1 and weighted[2]>=1 and weighted[3]<=summary['raw_rows']
 checks=[]
 for b in [0,15,31,47,63]:
  if b>=manifest['partitions']:continue
  m=root/'removed_mapping'/f'part_{b:03}.parquet';s=root/'staging'/f'bucket={b}'/'*.parquet'
  c.execute(f'CREATE OR REPLACE TEMP TABLE sampled AS SELECT * FROM read_parquet({q(m)}) LIMIT 20')
  c.execute(f'CREATE OR REPLACE TEMP TABLE original_rows AS SELECT * FROM read_parquet({q(s)}) WHERE source_row IN (SELECT removed_source_row FROM sampled UNION SELECT retained_source_row FROM sampled)')
  eq=' AND '.join(f'r.{i(x)} IS NOT DISTINCT FROM k.{i(x)}' for x in manifest['keys'])
  result=c.execute(f'''SELECT count(*), count(*) FILTER (WHERE ({eq}) AND r.id=m.removed_id AND k.id=m.retained_id AND k.source_row<r.source_row)
      FROM sampled m JOIN original_rows r ON m.removed_source_row=r.source_row JOIN original_rows k ON m.retained_source_row=k.source_row''').fetchone()
  assert result[0]==result[1]==c.execute('SELECT count(*) FROM sampled').fetchone()[0]
  checks.append(dict(partition=b,sampled_mappings=result[0],verified=result[1]))
 data=dict(passed=True,retained_parquet_rows=nk,removed_parquet_rows=nd,sum_duplicate_group_sizes=weighted[0],
           global_count_identity_passed=True,sampled_content_checks=checks,
           note='Global counts and multiplicities checked in full; source payload equality and earliest-row mapping checked for 100 saved mappings across five partitions. Partition assignment uses hash only for routing, full source-field equality for dedup.')
 (out/'dedup_verification.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(data,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
