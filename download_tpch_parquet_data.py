#!/usr/bin/env python

import duckdb
import pyarrow.parquet as pq
import os

scale_factor = 0.01 # scale_factor=1 <=> 1GB database

con = duckdb.connect(database=':memory:')
con.execute("INSTALL tpch; LOAD tpch")
con.execute(f"CALL dbgen(sf={scale_factor})")

queries = con.execute("FROM tpch_queries()").fetchall()
tables = con.execute("show tables").fetchall()

# Ensure output directories exist
os.makedirs('test_data/', exist_ok=True)
os.makedirs('test_sql/', exist_ok=True)

for (i, q) in queries:
    with open(f"test_sql/{i}.sql", "w") as file:
        file.writelines(q)
for (t,) in tables:
    res = con.query("SELECT * FROM " + t)
    pq.write_table(res.to_arrow_table(), f"test_data/{t}.parquet")
