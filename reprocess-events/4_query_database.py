import duckdb

conn = duckdb.connect(database='distance-stats.duckdb')
c = conn.cursor()

print(c.sql("SELECT * FROM dist_calc"))