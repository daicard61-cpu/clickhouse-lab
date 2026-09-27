#!/usr/bin/env python3
"""Measure serialized state size and accuracy for dense vs scattered bijective IDs."""
import csv
from pathlib import Path
import subprocess
import time
from benchmark import Client, FUNCTIONS, write_csv


def main():
    client=Client();rows=[]
    for cardinality in (1000,10000,100000,1000000):
        for layout in ('dense','scattered'):
            # Multiplication by an odd constant modulo 2**64 is a bijection, not a hash surrogate.
            expr='toUInt64(number+1)' if layout=='dense' else 'toUInt64((number+1)*toUInt64(11400714819323198485))'
            for name,function in FUNCTIONS.items():
                start=time.monotonic()
                sql='SELECT %sState(%s) FROM numbers(%d) FORMAT RowBinary'%(function,expr,cardinality)
                result=subprocess.run(client.command,input=sql.encode(),capture_output=True,timeout=120)
                if result.returncode:raise RuntimeError('state serialization probe failed')
                # RowBinary contains one serialized AggregateFunction state, no names/types header.
                length=len(result.stdout)
                estimate=int(client.scalar('SELECT %s(%s) FROM numbers(%d)'%(function,expr,cardinality)))
                rows.append(dict(cardinality=cardinality,id_layout=layout,method=name,state_serialized_bytes=length,
                                 estimate=estimate,error_pct=abs(estimate-cardinality)*100/cardinality,
                                 transport_and_serialize_s=time.monotonic()-start))
                if name in ('exact','bitmap'):assert estimate==cardinality
    target=Path(__file__).parent/'results'/'state_probe.csv'
    write_csv(target,rows)
    print('state probe complete: %s'%target)

if __name__=='__main__':main()
