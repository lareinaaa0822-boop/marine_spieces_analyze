"""Read-only audit of existing environment, depth schemas and sampled point values."""
import argparse, csv, json, re
from collections import Counter
from pathlib import Path
import numpy as np
import pandas as pd
import rasterio

def main():
    p=argparse.ArgumentParser(); p.add_argument('--out',required=True); a=p.parse_args()
    out=Path(a.out);out.mkdir(parents=True,exist_ok=True)
    dirs={v:Path('D:/ENV_TIF')/('PHY' if v in ['thetao','so'] else 'ENV_TIFBGC_resampled')/v for v in ['thetao','so','chl','no3','o2']}
    indices={}; schemas={}; inventory=[]
    for v,d in dirs.items():
        idx={}; dep={}; duplicates=[]
        for f in d.glob('*.tif'):
            m=re.fullmatch(rf'{v}_(\d{{4}})_L(\d+)_([\d.]+)m.tif',f.name)
            if not m: continue
            y,l,z=int(m[1]),int(m[2]),float(m[3])
            if (y,l) in idx: duplicates.append([y,l])
            idx[y,l]=f;dep.setdefault(l,set()).add(z)
        indices[v]=idx; schemas[v]={l:sorted(zs) for l,zs in dep.items()}
        inventory.append(dict(variable=v,files=len(idx),years=sorted({x[0] for x in idx}),layers=sorted(dep),
                              duplicate_year_layer_keys=duplicates,
                              missing_year_layers=[[y,l] for y in range(1993,2026) for l in range(1,51) if (y,l) not in idx]))
    (out/'environment_inventory.json').write_text(json.dumps(dict(inventory=inventory,depth_schemas=schemas),indent=2),encoding='utf-8')
    layers=[]
    for l in range(1,51):
        row={'layer':l}
        for v in dirs: row[v+'_filename_depth_m']=schemas[v].get(l,[None])[0]
        layers.append(row)
    pd.DataFrame(layers).to_csv(out/'depth_layer_mapping.csv',index=False,encoding='utf-8-sig')
    # Representative complete pixel arrays; no modification to source raster metadata.
    stats=[]; conflicts=[]
    for y in [1993,2020,2025]:
        for l in [1,20,40,50]:
            arrays={}; meta={}
            for v in dirs:
                f=indices[v].get((y,l))
                if f is None: continue
                with rasterio.open(f) as s:
                    ar=s.read(1);arrays[v]=ar
                    meta[v]=(s.shape,tuple(s.transform),str(s.crs))
                    va=ar[ar!=-1]
                    stats.append(dict(variable=v,year=y,layer=l,path=str(f),dtype=s.dtypes[0],nodata=s.nodata,
                                      scale=s.scales[0],offset=s.offsets[0],width=s.width,height=s.height,
                                      transform=repr(s.transform),n_minus1=int(np.count_nonzero(ar==-1)),
                                      n_zero=int(np.count_nonzero(ar==0)),n_negative_other=int(np.count_nonzero(ar<-1)),
                                      min_except_minus1=float(va.min()) if va.size else None,
                                      max_except_minus1=float(va.max()) if va.size else None))
            if 'thetao' in arrays and 'so' in arrays and meta['thetao']==meta['so']:
                mask=(arrays['thetao']==-1)&(arrays['so']!=-1)
                rr,cc=np.where(mask)
                sample=[]
                with rasterio.open(indices['thetao'][y,l]) as s:
                    for r,c in zip(rr[:10],cc[:10]):
                        x,yy=s.xy(int(r),int(c));sample.append(dict(row=int(r),col=int(c),lon=x,lat=yy,so=int(arrays['so'][r,c])))
                conflicts.append(dict(year=y,layer=l,thetao_minus1_so_valid=int(mask.sum()),examples=sample))
            print('raster_check',y,l,flush=True)
    pd.DataFrame(stats).to_csv(out/'environment_raster_checks.csv',index=False,encoding='utf-8-sig')
    (out/'temperature_minus1_candidates.json').write_text(json.dumps(conflicts,indent=2),encoding='utf-8')
    # Compare two depth conventions on stored, actual presence/background sample values.
    records=[];detailed=[]
    for sp in ['Abalistes_stellatus','Abudefduf_saxatilis','Acanthastrea_echinata']:
        sources={'presence':Path('G:/物种样本点处理后')/(sp+'.csv'),
                 'background':Path('D:/背景点带环境值')/(sp+'_background.csv')}
        for kind,f in sources.items():
            if not f.exists():continue
            df=pd.read_csv(f);df=df.sample(min(30,len(df)),random_state=42)
            for _,r in df.iterrows():
                if any(pd.isna(r.get(c)) for c in ['year','depth','longitude','latitude']):continue
                y=int(r.year);d=float(r.depth);x=float(r.longitude);yy=float(r.latitude)
                phy=min(schemas['thetao'],key=lambda l:abs(schemas['thetao'][l][0]-d))
                bgc=min(schemas['chl'],key=lambda l:abs(schemas['chl'][l][0]-d))
                item=dict(species=sp,kind=kind,source_path=str(f),source_dataframe_index=int(r.name),longitude=x,latitude=yy,year=y,depth=d,phy_layer=phy,bgc_independent_layer=bgc)
                for v in dirs:
                    for mode,l in [('shared_PHY_layer',phy),('independent_nearest',phy if v in ['thetao','so'] else bgc)]:
                        path=indices[v].get((y,l))
                        if path is None:continue
                        with rasterio.open(path) as s:
                            row,col=s.index(x,yy)
                            value=int(s.read(1,window=((row,row+1),(col,col+1)))[0,0]) if 0<=row<s.height and 0<=col<s.width else None
                        old=r.get(v)
                        detailed.append(dict(**item,variable=v,mode=mode,stored_value=old,reextracted_value=value,
                                             equal=bool(pd.notna(old) and value is not None and old==value)))
                records.append(item)
    ds=pd.DataFrame(detailed);ds.to_csv(out/'point_reextraction_checks.csv',index=False,encoding='utf-8-sig')
    if not ds.empty:ds.groupby(['kind','variable','mode']).agg(n=('equal','size'),equal=('equal','sum')).reset_index().to_csv(out/'point_reextraction_summary.csv',index=False,encoding='utf-8-sig')
    (out/'sample_counts.json').write_text(json.dumps(dict(points=len(records),different_depth_assignment=sum(r['phy_layer']!=r['bgc_independent_layer'] for r in records)),indent=2),encoding='utf-8')
    print('audit_complete',out,flush=True)

if __name__=='__main__':main()
