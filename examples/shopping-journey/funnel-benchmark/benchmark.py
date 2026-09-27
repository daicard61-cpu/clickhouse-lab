#!/usr/bin/env python3
"""Isolated ClickHouse funnel benchmark. Standard library only; kubectl transport."""
import argparse
import csv
import gzip
import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
STAGES = ('LANDING', 'OPEN', 'CLICK', 'CONVERSION')
FUNCTIONS = {'combined': 'uniqCombined64', 'exact': 'uniqExact', 'bitmap': 'groupBitmap'}
END = dt.datetime(2026, 9, 1)
WINDOWS = {'10m': 600, '1h': 3600, '1d': 86400, '30d': 30*86400, '90d': 90*86400}
COLUMNS = '''campaign_id UInt64, adgroup_id UInt64, placement_id UInt64,
tracking_id UInt64, stage LowCardinality(String), event_datetime DateTime64(3, 'UTC'),
ingested_at DateTime64(3, 'UTC'), source_type LowCardinality(String), message_id String,
customer_group_ids Array(UInt64), attributes String'''
# Explicit order shared by raw, ingress, unified, and stage tables.
EVENT_FIELDS = 'campaign_id,adgroup_id,placement_id,tracking_id,stage,event_datetime,ingested_at,source_type,message_id,customer_group_ids,attributes'


def quote(value):
    return "'" + str(value).replace('\\', '\\\\').replace("'", "\\'") + "'"


def timestamp(value):
    return "toDateTime64(%s,3,'UTC')" % quote(value.isoformat(sep=' ', timespec='milliseconds'))


def write_csv(path, rows):
    if not rows:
        path.write_text('')
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


class Client:
    def __init__(self):
        self.command = ['kubectl', '--context', os.getenv('LAB_KUBE_CONTEXT', 'kind-clickhouse-lab'),
            '-n', os.getenv('LAB_NAMESPACE', 'clickhouse'), 'exec', '-i',
            os.getenv('LAB_CLICKHOUSE_POD', 'chi-chi-cluster1-0-0-0'), '-c', 'clickhouse', '--',
            'clickhouse-client', '--multiquery', '--format', 'JSONEachRow',
            '--max_threads=2', '--max_memory_usage=1500000000', '--max_execution_time=90',
            '--max_bytes_before_external_group_by=300000000', '--async_insert=0',
            '--log_queries=1', '--log_queries_probability=1', '--use_query_cache=0']

    def sql(self, sql, timeout=3600):
        result = subprocess.run(self.command, input=sql, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            # Do not persist full stderr: it may include transport addresses or query input.
            codes = re.findall(r'(?:Code: |code )(\d+)', result.stderr)
            raise RuntimeError('ClickHouse command failed; exit=%s CH_codes=%s' % (result.returncode, codes))
        return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]

    def scalar(self, sql):
        return next(iter(self.sql(sql)[0].values()))


def ddl(client, file, **values):
    client.sql((ROOT/'ddl'/file).read_text().format(**values))


def schema(client, db, case):
    client.sql('CREATE DATABASE %s' % db)
    if case in ('raw', 'argmin'):
        client.sql('CREATE TABLE %s.funnel_event (%s) ENGINE=MergeTree PARTITION BY toYYYYMM(event_datetime) ORDER BY (placement_id,event_datetime,tracking_id)' % (db, COLUMNS))
        if case == 'argmin':
            ddl(client, 'baseline.sql', db=db)
        return
    client.sql('CREATE TABLE %s.ingress (%s) ENGINE=Null' % (db, COLUMNS))
    for stage in STAGES:
        ddl(client, 'event_layer.sql', db=db, stage=stage, stage_lower=stage.lower())
    function = 'uniqExact' if case == 'tracking' else FUNCTIONS[case]
    ddl(client, 'metric_1m.sql', db=db, function=function)
    ddl(client, 'metric_1h.sql', db=db, function=function)
    if case == 'tracking':
        ddl(client, 'tracking_state.sql', db=db)
    for stage in STAGES:
        table = stage.lower() + '_event'
        if case == 'tracking':
            sql = '''CREATE MATERIALIZED VIEW {db}.track_{table} TO {db}.funnel_tracking_state AS
                SELECT placement_id,tracking_id,stage,min(event_datetime) AS first_event_datetime
                FROM {db}.{table} GROUP BY placement_id,tracking_id,stage'''
        else:
            sql = '''CREATE MATERIALIZED VIEW {db}.metric_{table} TO {db}.funnel_metric_1m AS
                SELECT campaign_id,adgroup_id,placement_id,toStartOfMinute(event_datetime) AS metric_time,
                       stage,{function}State(tracking_id) AS tracking_state
                FROM {db}.{table} GROUP BY campaign_id,adgroup_id,placement_id,metric_time,stage'''
        client.sql(sql.format(db=db, table=table, function=function))


def event_union(db):
    return '(' + ' UNION ALL '.join('SELECT * FROM %s.%s_event' % (db,s.lower()) for s in STAGES) + ')'


def event_condition(count, start, end):
    ids = ','.join(map(str, range(1,count+1)))
    return 'placement_id IN (%s) AND event_datetime >= %s AND event_datetime < %s' % (ids, timestamp(start), timestamp(end))


def metric_query(db, function, grain, count, start, end):
    cond = event_condition(count,start,end).replace('event_datetime','metric_time')
    return 'SELECT placement_id,stage,%sMerge(tracking_state) AS n FROM %s.funnel_metric_%s WHERE %s GROUP BY placement_id,stage' % (function,db,grain,cond)


def hybrid_query(db, function, count, start, end):
    """Merge full hours and raw boundary states, never finalized distinct numbers."""
    lo = start.replace(minute=0,second=0,microsecond=0)
    if lo < start:
        lo += dt.timedelta(hours=1)
    hi = end.replace(minute=0,second=0,microsecond=0)
    ids = ','.join(map(str, range(1,count+1)))
    branches = []
    if lo < hi:
        branches.append('SELECT placement_id,stage,%sMergeState(tracking_state) AS s FROM %s.funnel_metric_1h WHERE placement_id IN (%s) AND metric_time >= %s AND metric_time < %s GROUP BY placement_id,stage' % (function,db,ids,timestamp(lo),timestamp(hi)))
        ranges = [(start,lo),(hi,end)]
    else:
        ranges = [(start,end)]
    for a,b in ranges:
        if a >= b:
            continue
        for stage in STAGES:
            branches.append('SELECT placement_id,stage,%sState(tracking_id) AS s FROM %s.%s_event WHERE %s GROUP BY placement_id,stage' % (function,db,stage.lower(),event_condition(count,a,b)))
    return 'SELECT placement_id,stage,%sMerge(s) AS n FROM (%s) GROUP BY placement_id,stage' % (function,' UNION ALL '.join(branches))


def snapshot_sql(db):
    # From immutable input. No incremental MV from min state: previous minima may change.
    return '''INSERT INTO {db}.funnel_metric_1m
        SELECT toUInt64(1+intDiv(placement_id-1,100)) campaign_id,
               toUInt64(1+intDiv(placement_id-1,10)) adgroup_id,
               placement_id,toStartOfMinute(first_time) metric_time,stage,
               uniqExactState(tracking_id) tracking_state
        FROM (SELECT placement_id,tracking_id,stage,min(first_event_datetime) first_time
              FROM {db}.funnel_tracking_state GROUP BY placement_id,tracking_id,stage)
        GROUP BY campaign_id,adgroup_id,placement_id,metric_time,stage'''.format(db=db)


def query_set(db, case, count, start, end):
    cond = event_condition(count,start,end)
    if case == 'raw':
        return {
          'raw_unique': 'SELECT placement_id,stage,uniqExact(tracking_id) AS n FROM %s.funnel_event WHERE %s GROUP BY placement_id,stage' % (db,cond),
          'raw_window_argmin': '''SELECT placement_id,stage,count() AS n FROM (
              SELECT placement_id,tracking_id,stage,argMin(tuple(campaign_id,adgroup_id,event_datetime,ingested_at,toString(source_type),message_id,customer_group_ids,attributes),tuple(ingested_at,message_id)) representative
              FROM %s.funnel_event WHERE %s GROUP BY placement_id,tracking_id,stage
              HAVING length(toString(representative))>0) GROUP BY placement_id,stage''' % (db,cond)}
    if case in ('argmin','tracking'):
        ids = ','.join(map(str,range(1,count+1)))
        first = 'argMinMerge(representative).3' if case == 'argmin' else 'min(first_event_datetime)'
        name = 'argmin_first_received' if case == 'argmin' else 'tracking_first_event'
        queries = {name: '''SELECT placement_id,stage,count() AS n FROM (
          SELECT placement_id,tracking_id,stage,%s AS event_datetime FROM %s.funnel_tracking_state
          WHERE placement_id IN (%s) GROUP BY placement_id,tracking_id,stage)
          WHERE event_datetime >= %s AND event_datetime < %s GROUP BY placement_id,stage''' % (first,db,ids,timestamp(start),timestamp(end))}
        if case == 'tracking':
            # 10m cannot use coarse 1h; all other benchmark windows align to hours.
            queries['tracking_snapshot'] = metric_query(db,'uniqExact','1m' if end-start < dt.timedelta(hours=1) else '1h',count,start,end)
        return queries
    function=FUNCTIONS[case]
    return {case+'_1m':metric_query(db,function,'1m',count,start,end),
            case+'_hybrid':hybrid_query(db,function,count,start,end)}


class Run:
    def __init__(self,args):
        self.args=args
        self.client=Client()
        self.run_id=args.run_id or dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        if not re.fullmatch('[a-zA-Z0-9_]+',self.run_id):
            raise ValueError('run-id must use letters, digits, underscores')
        self.prefix='fb_'+self.run_id.lower()
        self.out=ROOT/'results'/self.run_id
        self.out.mkdir(parents=True,exist_ok=False)
        self.logs=[]; self.storage=[]; self.parts=[]; self.merge_samples=[]; self.accuracy=[]; self.inserts=[]
        self.meta={}; self.truth={}; self.first_truth={}; self.sequence=0
        self.stop=threading.Event()
        self.created=[]

    def tagged(self,sql,**meta):
        self.sequence+=1
        tag=self.prefix+'_'+str(self.sequence)
        self.meta[tag]=meta
        return 'SET log_comment=%s;\n%s;\n' % (quote(tag),sql),tag

    def save(self):
        for name,rows in [('query_log',self.logs),('storage',self.storage),('part_log',self.parts),('merge_samples',self.merge_samples),('accuracy',self.accuracy),('insert_wall',self.inserts)]:
            write_csv(self.out/(name+'.csv'),rows)
        (self.out/'samples.json').write_text(json.dumps(self.meta,indent=2))

    def collect_logs(self):
        self.client.sql('SYSTEM FLUSH LOGS')
        data=self.client.sql('''SELECT log_comment,type,query_duration_ms,read_rows,read_bytes,result_rows,memory_usage,written_rows,
          ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds'] AS cpu_us,
          ProfileEvents['ExternalAggregationCompressedBytes'] AS spill_bytes,
          exception_code FROM system.query_log WHERE startsWith(log_comment,%s) AND is_initial_query=1 AND query_kind IN ('Select','Insert') AND type != 'QueryStart' ''' % quote(self.prefix+'_'))
        self.logs=[dict(self.meta.get(x.pop('log_comment'),{}),**x) for x in data if x['type'] in ('QueryFinish','ExceptionWhileProcessing','ExceptionBeforeStart')]
        self.save()

    def collect_storage(self,db,scale,case,phase):
        rows=self.client.sql('''SELECT table,count() active_parts,sum(rows) rows,sum(bytes_on_disk) disk_bytes,
          sum(data_compressed_bytes) compressed_bytes,sum(data_uncompressed_bytes) uncompressed_bytes
          FROM system.parts WHERE active AND database=%s GROUP BY table ORDER BY table''' % quote(db))
        for x in rows:self.storage.append(dict(scale=scale,case=case,phase=phase,**x))
        states=self.client.sql('''SELECT table,column,sum(column_data_compressed_bytes) compressed_bytes,
          sum(column_data_uncompressed_bytes) uncompressed_bytes FROM system.parts_columns
          WHERE active AND database=%s AND (column IN ('tracking_state','representative','first_event_datetime','first_ingested_at'))
          GROUP BY table,column ORDER BY table,column''' % quote(db))
        for x in states:self.storage.append(dict(scale=scale,case=case,phase=phase,kind='state_column',**x))
        self.client.sql('SYSTEM FLUSH LOGS')
        rows=self.client.sql('''SELECT table,toString(event_type) event_type,count() operations,sum(duration_ms) duration_ms,
          sum(read_rows) read_rows,sum(read_bytes) read_bytes,sum(size_in_bytes) written_bytes,
          max(peak_memory_usage) peak_memory,
          sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']) cpu_us,
          countIf(error!=0) errors FROM system.part_log WHERE database=%s GROUP BY table,event_type''' % quote(db))
        for x in rows:self.parts.append(dict(scale=scale,case=case,phase=phase,**x))
        self.save()

    def sampler(self,db,scale,case):
        while not self.stop.is_set():
            try:
                samples=self.client.sql('''SELECT table,elapsed,progress,num_parts,memory_usage,rows_read,rows_written FROM system.merges WHERE database=%s''' % quote(db),timeout=15)
                for s in samples:self.merge_samples.append(dict(scale=scale,case=case,sampled_at=dt.datetime.now(dt.timezone.utc).isoformat(),**s))
            except Exception:
                # A failed monitor sample must not hide a failed benchmark command.
                self.merge_samples.append(dict(scale=scale,case=case,sample_error=1))
            self.stop.wait(1)

    def create(self,db):
        if self.client.scalar('EXISTS DATABASE '+db):
            raise RuntimeError('database already exists; refusing to overwrite')
        self.created.append(db)

    def generate(self,scale):
        db=self.prefix+'_source_'+str(scale)
        self.create(db)
        self.client.sql('CREATE DATABASE %s; CREATE TABLE %s.raw (seq UInt64,%s) ENGINE=MergeTree ORDER BY seq' % (db,db,COLUMNS))
        for offset in range(0,scale,self.args.batch):
            sql=(ROOT/'generate.sql').read_text().format(db=db,tracking_count=scale//16,offset=offset,count=min(self.args.batch,scale-offset))
            self.client.sql(sql)
        assert int(self.client.scalar('SELECT count() FROM '+db+'.raw'))==scale
        return db

    def matrix(self,db,scale,case):
        statements=[]; tags={}
        for count in self.args.placements:
            for window,seconds in WINDOWS.items():
                start=END-dt.timedelta(seconds=seconds)
                for name,query in query_set(db,case,count,start,END).items():
                    for rep in range(self.args.repeats+1):
                        meta=dict(scale=scale,case=case,operation='query',method=name,placements=count,window=window,rep=rep,warmup=int(rep==0))
                        sql,tag=self.tagged(query,**meta)
                        # Attach tag to result rows while preserving actual service GROUP BY.
                        query_with_tag='SELECT '+quote(tag)+' AS sample_id, * FROM ('+query+')'
                        statements.append('SET log_comment=%s; %s FORMAT JSONEachRow;' % (quote(tag),query_with_tag))
                        tags[tag]=meta
        with gzip.open(self.out/(str(scale)+'_'+case+'_queries.sql.gz'),'wt') as saved_sql:
            saved_sql.write('\n'.join(statements))
        data=self.client.sql('\n'.join(statements))
        results={tag:{} for tag in tags}
        for row in data:
            results[row['sample_id']][(int(row['placement_id']),row['stage'])]=int(row['n'])
        for tag,meta in tags.items():
            if meta['rep']!=self.args.repeats:continue
            key=(scale,meta['placements'],meta['window'])
            actual=results[tag]
            if meta['method']=='raw_unique':self.truth[key]=actual
            if meta['method']=='tracking_first_event':self.first_truth[key]=actual
            truth=self.truth[key]
            comparable=case not in ('argmin','tracking')
            if meta['method']=='tracking_snapshot':truth=self.first_truth[key];comparable=True
            pairs=set(truth)|set(actual)
            errors=[abs(actual.get(k,0)-truth.get(k,0))/max(1,truth.get(k,0))*100 for k in pairs]
            row=dict(meta,compared_to='first_event' if meta['method']=='tracking_snapshot' else 'window_unique',same_semantics=int(comparable),groups=len(pairs),different_groups=sum(actual.get(k,0)!=truth.get(k,0) for k in pairs),max_error_pct=max(errors,default=0),mean_error_pct=sum(errors)/max(1,len(errors)))
            self.accuracy.append(row)
            if comparable and case!='combined' and row['different_groups']:
                self.save();raise AssertionError('exact result mismatch: '+json.dumps(row))
        self.collect_logs()

    def execute_case(self,source,scale,case):
        db=self.prefix+'_'+str(scale)+'_'+case
        self.create(db);schema(self.client,db,case)
        self.stop.clear();thread=threading.Thread(target=self.sampler,args=(db,scale,case),daemon=True);thread.start()
        try:
            target='funnel_event' if case in ('raw','argmin') else 'ingress'
            t=time.monotonic()
            batch_sql=[]
            for offset in range(0,scale,self.args.batch):
                query='INSERT INTO %s.%s SELECT %s FROM %s.raw WHERE seq >= %d AND seq < %d' % (db,target,EVENT_FIELDS,source,offset,offset+self.args.batch)
                sql,_=self.tagged(query,scale=scale,case=case,operation='insert',batch_offset=offset,batch_rows=min(self.args.batch,scale-offset))
                batch_sql.append(sql)
            self.client.sql('\n'.join(batch_sql))
            elapsed=time.monotonic()-t
            self.inserts.append(dict(scale=scale,case=case,wall_s=elapsed,rows_per_wall_s=scale/elapsed))
            self.collect_storage(db,scale,case,'after_insert')
            if case=='tracking':
                sql,_=self.tagged(snapshot_sql(db),scale=scale,case=case,operation='snapshot')
                self.client.sql(sql)
            self.matrix(db,scale,case)
            self.collect_storage(db,scale,case,'after_queries')
        finally:
            self.stop.set();thread.join(timeout=20);self.collect_logs()
        return db

    def run(self):
        metadata=self.client.sql('SELECT version() version,timezone() timezone')
        metadata += self.client.sql("SELECT name,value FROM system.settings WHERE name IN ('max_threads','max_memory_usage','max_execution_time','async_insert','max_bytes_before_external_group_by')")
        (self.out/'environment.json').write_text(json.dumps(dict(metadata=metadata,scale=self.args.scales,placements=self.args.placements,repeats=self.args.repeats,batch=self.args.batch,scope='one pod, non-replicated engines, sequential cases; shared host with existing cluster'),indent=2))
        for scale in self.args.scales:
            print('Generating %s events across 90 days' % scale,flush=True)
            free=int(self.client.scalar('SELECT min(free_space) FROM system.disks'))
            if free < 10*1024**3:raise RuntimeError('less than 10 GiB free; stopping before new scale')
            source=self.generate(scale)
            for case in ('raw','argmin','tracking','combined','exact','bitmap'):
                print('%s: %s' % (scale,case),flush=True)
                self.execute_case(source,scale,case)
                print('completed %s: %s' % (scale,case),flush=True)
        self.save()
        print('Results: '+str(self.out),flush=True)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--scales',nargs='+',type=int,default=[1000000,5000000,10000000])
    p.add_argument('--placements',nargs='+',type=int,default=[1,10,100,1000,2000])
    p.add_argument('--batch',type=int,default=100000)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--run-id')
    p.add_argument('--skip-correctness',action='store_true',help='skip separate boundary fixtures')
    a=p.parse_args()
    if any(n<=0 or n%16 for n in a.scales) or a.batch<=0 or a.repeats<1 or any(n<1 or n>2000 for n in a.placements):
        p.error('positive scales divisible by 16, positive batch/repeats, placements 1..2000 required')
    r=Run(a)
    try:
        if not a.skip_correctness:
            from verify import main as verify
            verify()
        r.run()
        subprocess.run([sys.executable,'-B',str(ROOT/'report.py'),str(r.out)],check=True)
    except Exception as exc:
        r.save();(r.out/'failure.json').write_text(json.dumps({'error':str(exc),'created_databases':r.created},indent=2))
        raise

if __name__=='__main__':main()
