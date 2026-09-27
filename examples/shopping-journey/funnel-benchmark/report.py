#!/usr/bin/env python3
"""Build auditable median tables from query_log.csv; never manufacture results."""
import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median
from benchmark import write_csv


def main():
    p=argparse.ArgumentParser();p.add_argument('run');a=p.parse_args()
    root=Path(a.run)
    logs=list(csv.DictReader((root/'query_log.csv').open()))
    storage=list(csv.DictReader((root/'storage.csv').open()))
    wall={(r['scale'],r['case']):r for r in csv.DictReader((root/'insert_wall.csv').open())}
    disk=defaultdict(int)
    for r in storage:
        if r['phase']=='after_queries' and not r.get('kind'):
            disk[(r['scale'],r['case'])]+=int(r['disk_bytes'])
    groups=defaultdict(list)
    for r in logs:
        if r['operation']=='query' and r['type']=='QueryFinish' and r.get('warmup')=='0':
            groups[tuple(r[k] for k in ('scale','case','method','placements','window'))].append(r)
    queries=[]
    for key,rows in groups.items():
        scale,case,method,count,window=key
        out=dict(scale=int(scale),case=case,method=method,placements=int(count),window=window,samples=len(rows))
        for metric in ('query_duration_ms','read_rows','read_bytes','result_rows','memory_usage','cpu_us'):
            out[metric+'_median']=median(int(r[metric]) for r in rows)
            out[metric+'_max']=max(int(r[metric]) for r in rows)
        out['disk_bytes']=disk[(scale,case)]
        queries.append(out)
    queries.sort(key=lambda r:(r['scale'],r['method'],r['placements'],r['window']))
    write_csv(root/'query_summary.csv',queries)
    groups=defaultdict(list)
    for r in logs:
        if r['operation']=='insert' and r['type']=='QueryFinish':groups[(r['scale'],r['case'])].append(r)
    inserts=[]
    for (scale,case),rows in groups.items():
        elapsed=sum(int(r['query_duration_ms']) for r in rows)/1000
        inserts.append(dict(scale=int(scale),case=case,batches=len(rows),server_insert_s=elapsed,
          server_rows_s=int(scale)/max(elapsed,0.000001),cpu_s=sum(int(r['cpu_us']) for r in rows)/1000000,
          peak_memory_bytes=max(int(r['memory_usage']) for r in rows),disk_bytes=disk[(scale,case)],
          wall_s=float(wall[(scale,case)]['wall_s']),wall_rows_s=float(wall[(scale,case)]['rows_per_wall_s'])))
    inserts.sort(key=lambda r:(r['scale'],r['case']))
    write_csv(root/'insert_summary.csv',inserts)
    snapshots=[{k:r[k] for k in ('scale','case','query_duration_ms','read_rows','read_bytes','memory_usage','cpu_us','spill_bytes')}
               for r in logs if r['operation']=='snapshot' and r['type']=='QueryFinish']
    snapshots.sort(key=lambda r:int(r['scale']))
    write_csv(root/'snapshot_summary.csv',snapshots)
    lines=['# Query matrix (measured medians)','','The table uses server query_log times; three samples are exploratory, not a production p99. Disk includes event and derived tables for that case, excludes the shared normalized raw source.','',
      '| Structure | Rows | Placements | Window | elapsed ms | CPU ms | read_rows | read_bytes | result_rows | memory MiB | disk MiB |',
      '|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in queries:
        lines.append('| %s | %s | %s | %s | %.1f | %.2f | %.0f | %.0f | %.0f | %.2f | %.2f |'%(
          r['method'],r['scale'],r['placements'],r['window'],r['query_duration_ms_median'],r['cpu_us_median']/1000,
          r['read_rows_median'],r['read_bytes_median'],r['result_rows_median'],r['memory_usage_median']/2**20,r['disk_bytes']/2**20))
    (root/'QUERY_MATRIX.md').write_text('\n'.join(lines)+'\n')
    print('Generated %d query comparisons and %d insertion summaries'%(len(queries),len(inserts)))

if __name__=='__main__':main()
