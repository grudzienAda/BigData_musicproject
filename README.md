# BigData_musicproject

### Czemu zmieniliśmy koncepcję zapisywania danych w formacie .avro na format .parquet?
Avro is a Row based format. If you want to retrieve the data as a whole you can use Avro
Parquet is a Column based format. If your data consists of a lot of columns but you are interested in a subset of columns then you can use Parquet
HBase is useful when frequent updating of data is involved. Avro is fast in retrieval, Parquet is much faster.
