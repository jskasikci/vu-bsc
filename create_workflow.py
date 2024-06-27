import os
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import igraph as ig
import numpy as np

schema_meta = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("start_time", pa.timestamp("ms"), nullable=False),
    pa.field("stop_time", pa.timestamp("ms"), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_capacity", pa.float64(), nullable=False),
    pa.field("mem_capacity", pa.int64(), nullable=False),
    pa.field("dependencies", pa.string(), nullable=False)
])

schema_trace = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("ms"), nullable=False),
    pa.field("duration", pa.int64(), nullable=False),
    pa.field("cpu_count", pa.int32(), nullable=False),
    pa.field("cpu_usage", pa.float64(), nullable=False)
])

def generate_meta_data(graph, start_date):
    meta_data = []
    for i, node in enumerate(graph.vs):
        id_str = str(node.index)
        start_time = start_date
        duration_minutes = np.random.randint(10, 120 + 1)
        stop_time = start_time + timedelta(minutes=duration_minutes)
        dependencies = ",".join(map(str, graph.neighbors(node, mode="out"))) if graph.neighbors(node, mode="out") else ""
        cpu_count = (i % 2) + 1
        cpu_capacity = 100
        memory_capacity = 500

        meta_data.append([id_str, start_time, stop_time, cpu_count, cpu_capacity, memory_capacity, dependencies])
    return meta_data

def generate_trace_data(meta_data):
    trace_data = []
    trace_count = 5
    
    for meta_row in meta_data:
        node_id = meta_row[0]
        start_time = meta_row[1]
        stop_time = meta_row[2]
        cpu_count = meta_row[3]
        cpu_usage = meta_row[4]
        
        duration = (stop_time - start_time).total_seconds() * 1000
        trace_duration = duration / trace_count

        for j in range(trace_count):
            timestamp = start_time + timedelta(milliseconds=j * trace_duration)

            trace_data.append([node_id, timestamp, trace_duration, cpu_count, cpu_usage])
    return trace_data

def write_parquet(meta_data, trace_data):
    meta_columns = ["id", "start_time", "stop_time", "cpu_count", "cpu_capacity", "mem_capacity", "dependencies"]
    trace_columns = ["id", "timestamp", "duration", "cpu_count", "cpu_usage"]
    
    df_meta = pd.DataFrame(meta_data, columns=meta_columns)
    pa_meta = pa.Table.from_pandas(df_meta, schema=schema_meta, preserve_index=False)
    
    df_trace = pd.DataFrame(trace_data, columns=trace_columns)
    pa_trace = pa.Table.from_pandas(df_trace, schema=schema_trace, preserve_index=False)
    
    output_folder = f"traces/bitbrains-small"
    if not os.path.exists(f"{output_folder}/trace"):
        os.makedirs(f"{output_folder}/trace")
    
    try:
        pq.write_table(pa_meta, f"{output_folder}/trace/meta.parquet")
        pq.write_table(pa_trace, f"{output_folder}/trace/trace.parquet")
        print(f"Parquet files written successfully for workflow.")
    except Exception as e:
        print(f"Error writing Parquet files for {graph_type}: {e}")

def create_random_dag(num_nodes, num_edges):
    graph = ig.Graph.Erdos_Renyi(n=num_nodes, m=num_edges, directed=False, loops=False)
    graph.to_directed(mode="acyclic")
    
    graph.vs["name"] = [str(i) for i in range(num_nodes)]
    
    return graph

def create_and_process_dag():
    start_date = datetime.strptime("2024-02-01", "%Y-%m-%d")
    
    num_nodes = 200
    num_edges = 100
    ig_dag = create_random_dag(num_nodes, num_edges)

    meta = generate_meta_data(ig_dag, start_date)
    trace = generate_trace_data(meta)

    write_parquet(meta, trace)

create_and_process_dag()
