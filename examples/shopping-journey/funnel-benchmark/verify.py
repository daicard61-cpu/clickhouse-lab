#!/usr/bin/env python3
"""Boundary, replay, late arrival and cross-bucket unique assertions on real CH."""
import datetime as dt
import json
from pathlib import Path
import time
from benchmark import Client, FUNCTIONS, STAGES, schema, quote, metric_query, hybrid_query


def main():
    client=Client()
    suffix=str(time.time_ns())
    results=[]
    # Tracking 1 repeats in two minutes, two hours and two dates; placement 2 reuses it.
    fixture=[(1,1,'CLICK','2026-08-31 12:01:00'),
             (1,1,'CLICK','2026-08-31 12:30:00'),
             (1,1,'CLICK','2026-08-31 13:05:00'),
             (1,1,'CLICK','2026-08-30 12:00:00'),
             (1,2,'CLICK','2026-08-31 12:30:30'),
             (2,1,'CLICK','2026-08-31 12:02:00')]
    fixture += [(1,1,s,'2026-08-31 12:01:30') for s in STAGES if s!='CLICK']
    windows=[('minute_aligned',dt.datetime(2026,8,31,12),dt.datetime(2026,8,31,13)),
             ('cross_hour',dt.datetime(2026,8,31,12),dt.datetime(2026,8,31,14)),
             ('cross_day',dt.datetime(2026,8,30),dt.datetime(2026,9,1)),
             ('partial_hour',dt.datetime(2026,8,31,12,15),dt.datetime(2026,8,31,13,6)),
             ('partial_second',dt.datetime(2026,8,31,12,30,1),dt.datetime(2026,8,31,12,30,31)),
             ('partial_millisecond',dt.datetime(2026,8,31,12,30,30,500000),dt.datetime(2026,8,31,12,30,31)),
             ('empty',dt.datetime(2026,8,29),dt.datetime(2026,8,30))]
    for case,function in FUNCTIONS.items():
        db='fb_verify_'+suffix+'_'+case
        schema(client,db,case)
        try:
            for phase,rows in [('initial',fixture),('replay',fixture),('late',[(1,1,'CLICK','2026-08-31 11:59:59')])]:
                # Each INSERT triggers an independent cascade; same key across blocks.
                for i,(p,t,s,when) in enumerate(rows):
                    values=[str(1),str(1),str(p),str(t),quote(s),quote(when),quote('2026-09-01 00:00:00'),quote('synthetic'),quote('v'+str(i)),'[1,2]',quote('test')]
                    client.sql('INSERT INTO %s.ingress VALUES (%s)'%(db,','.join(values)))
                all_rows=fixture+([(1,1,'CLICK','2026-08-31 11:59:59')] if phase=='late' else [])
                for name,start,end in windows:
                    expected={}
                    for p,t,s,when in all_rows:
                        if start<=dt.datetime.fromisoformat(when)<end:expected.setdefault((p,s),set()).add(t)
                    expected={k:len(v) for k,v in expected.items()}
                    methods={'hybrid':hybrid_query(db,function,2,start,end)}
                    if name in ('minute_aligned','cross_hour','cross_day','empty'):
                        methods['1m']=metric_query(db,function,'1m',2,start,end)
                        methods['1h']=metric_query(db,function,'1h',2,start,end)
                    for method,sql in methods.items():
                        actual={(int(r['placement_id']),r['stage']):int(r['n']) for r in client.sql(sql)}
                        assert actual==expected,(case,phase,name,method,actual,expected)
                        results.append(dict(case=case,phase=phase,window=name,method=method,passed=True))
            # Show the counterexample to summing finalized minute distincts.
            value=client.scalar("SELECT sum(n) FROM (SELECT metric_time,%sMerge(tracking_state) n FROM %s.funnel_metric_1m WHERE placement_id=1 AND stage='CLICK' AND metric_time >= toDateTime('2026-08-31 12:00:00','UTC') AND metric_time < toDateTime('2026-08-31 13:00:00','UTC') GROUP BY metric_time)"%(function,db))
            assert int(value)==3
            client.sql('OPTIMIZE TABLE %s.funnel_metric_1m FINAL; OPTIMIZE TABLE %s.funnel_metric_1h FINAL'%(db,db))
            value=client.sql(metric_query(db,function,'1h',1,dt.datetime(2026,8,31,12),dt.datetime(2026,8,31,13)))
            assert next(int(r['n']) for r in value if r['stage']=='CLICK')==2
            # Wide service response and empty stages also use correct merge combinators.
            wide=client.sql("SELECT %sMergeIf(tracking_state,stage='CLICK') click,%sMergeIf(tracking_state,stage='MISSING') missing FROM %s.funnel_metric_1h WHERE placement_id=1 AND metric_time=toDateTime('2026-08-31 12:00:00','UTC')"%(function,function,db))[0]
            assert int(wide['click'])==2 and int(wide['missing'])==0
            results.append(dict(case=case,phase='after_merge',window='1h',method='wide_merge_if',passed=True,wrong_minute_sum=3,correct_hour_unique=2))
        finally:
            client.sql('DROP DATABASE %s SYNC'%db)
    output=Path(__file__).parent/'results'/'correctness.json'
    output.write_text(json.dumps(results,indent=2))
    print('%d real ClickHouse assertions passed'%len(results))

if __name__=='__main__':main()
