#!/usr/bin/env python

import duckdb
import pyarrow.parquet as pq

scale_factor = 0.01 # scale_factor=1 <=> 1GB database

con = duckdb.connect(database=':memory:')
con.execute("INSTALL tpch; LOAD tpch")
con.execute(f"CALL dbgen(sf={scale_factor})")

queries = con.execute("FROM tpch_queries()").fetchall()
tables = con.execute("show tables").fetchall()

for (i, q) in queries:
    with open(f"{i}.sql", "w") as file:
        file.writelines(q)
for (t,) in tables:
    res = con.query("SELECT * FROM " + t)
    pq.write_table(res.to_arrow_table(), t + ".parquet")
