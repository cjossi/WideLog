import polars as pl

df = pl.read_parquet("/Users/corentin/Documents/EPFL/PDM/WideLog/data/processed/tests_index.parquet")
print(df.columns)
print(df.head(10))