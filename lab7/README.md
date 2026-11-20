Lab 7: Bronze to silver with Spark DataFrames

Maxime CURIE

Environment: Onyxia vscode-pyspark service, PySpark shell in local mode, Spark 4.1.1, Scala 2.13, Java 21, hadoop-aws-3.4.2, Iceberg runtime iceberg-spark-runtime-4.1_2.13:1.11.0.

The Iceberg metadata files are committed in the warehouse/ folder at the root of the repository. The data files stay on S3.

1. Spark session with S3 access

The PySpark shell was started with the Iceberg package. Two small changes were needed:

Questions

What happens to a running job if the service restarts? The job stops, because Spark runs inside the service. Everything in memory is lost. The credentials were copied into the session when it was created. After a restart, or when the token expires (after about 12 hours), the session must be created again with new credentials.

Why does SimpleAWSCredentialsProvider fail here? It only sends the access key and the secret key. Our keys are temporary and also need a session token. Without the token, S3 refuses the request. TemporaryAWSCredentialsProvider sends the token too.

2. Bronze layer

The two CSV files are read from S3. Thanks to multiLine=True, each address is kept as one value, with a line break inside:

text
908 Jennifer Squares\nRobinsonshire, KY 01352

The schema has 7 columns: 6 strings and birthdate as a date.

3. Silver layer
Users

stg_users renames the columns, sets their types and splits the address into street, city, state and zip_code. When there is no city, the empty string is replaced with NULL.

text
|username     |street              |city         |state|zip_code|
|garzaanthony |908 Jennifer Squares|Robinsonshire|KY   |01352   |
|blairamanda  |Unit 6184 Box 9593  |NULL         |AP   |09617   |
Orders
Step	Rows
Original orders	2829
After adding 3 duplicates	2832
After removing duplicates with row_number()	2829

The Spark SQL version gives the same number of rows and the same columns.

Check of the address parsing
text
|state|users|missing_city|
|   AE|    3|           3|
|   FM|    3|           0|
|   AP|    2|           2|
Questions

Explain the output. The query counts the users of each state, and how many have no city. Each state has only 1 to 3 users, because there are only 50 users. Only AE and AP have missing cities. These are military codes, and military addresses have no city. So the parsing works well.

DataFrame or SQL? SQL is better for a quick exploration: it is short and easy to read. The DataFrame API is better for code shared with colleagues: it is Python, so it can be split into functions, tested and reused. Both give the same execution plan, so the speed is the same.

Where else can the empty string of regexp_extract() cause a problem? In state and zip_code. They also use regexp_extract() and were not fixed. If an address has an unexpected format, they become an empty string instead of NULL. These rows would then form a fake group in a GROUP BY state, and a "not null" check would not see them.

4. Silver layer as Parquet
text
silver/dbt/orders.parquet                                  72466
silver/dbt/users.parquet                                    7018
silver/spark/orders.parquet/_SUCCESS                       11798
silver/spark/orders.parquet/part-00000-...snappy.parquet  139556
silver/spark/users.parquet/_SUCCESS                        11783
silver/spark/users.parquet/part-00000-...snappy.parquet     9580

dbt writes one file per model. Spark writes one folder per model, with a _SUCCESS file and the data files. Reading the files back gives 50 users and 2829 orders.

Questions

How many part files, and why? One per folder. Spark writes one file per partition, and both tables have only one partition. The users file is read with multiLine=True, which cannot be split, so it gives one partition. For the orders, Spark merged its partitions into one because the data is very small.

What do you lose with coalesce(1) on a large dataset? You lose parallelism. Only one task writes all the data, which is slow and can run out of memory. Reading one huge file is also slower than reading several files in parallel.

5. Silver layer as Iceberg tables
text
warehouse/silver/orders/data/00000-...parquet                    84055
warehouse/silver/orders/metadata/55f544d2-...-m0.avro             7346
warehouse/silver/orders/metadata/snap-675346543749128507-...avro  4455
warehouse/silver/orders/metadata/v1.metadata.json                 1926
warehouse/silver/orders/metadata/version-hint.text                   1

The Iceberg data file is smaller than the Spark Parquet file (84 KB against 140 KB) because Iceberg uses a stronger compression (zstd) by default.

The metadata tables show one snapshot (overwrite) and one data file with 2829 rows. Reading the tables back gives 50 users and 2829 orders.

Questions

Which files hold the data, and which describe the table? In both folders, the data is in Parquet files. The Parquet folder has nothing that describes the table: _SUCCESS only says the write is finished. The Iceberg folder has a metadata/ folder: the JSON file gives the schema and the versions, the Avro files list the data files, and version-hint.text gives the current version.

What does a reader see if another job overwrites the data during the read? With the Parquet folder, the reader can see a broken state: missing files, a mix of old and new files, or an empty folder. With Iceberg, the new job writes new files next to the old ones, then switches to the new version in one step. The reader sees either the old version or the new one, never a mix.

What can go wrong if two jobs commit at the same time? Here the current version is just a file in the bucket. Two jobs can both write version 2, and one of them is lost without any error. A catalog service (like an Iceberg REST catalog, Nessie or AWS Glue) changes the current version safely: a commit only works if nobody changed the table in the meantime, otherwise it is refused and retried.

6. Commit of the Iceberg metadata

The warehouse folder was copied from S3 to the repository. Only the 8 metadata files (4 per table) were committed. The data files were not.

7. Comparison with the dbt lab
Engine	Orders	Quantity	Users
dbt	2829	8615	50
Spark	2829	8615	50

Both engines build the same silver layer.

Column	dbt	Spark
order_id, user_id	uuid	varchar
ordered_at	timestamp with time zone	timestamp
quantity	integer	integer
product	varchar	varchar
Questions

What differs between the two schemas? The ids are uuid with dbt and text with Spark, because Spark has no UUID type. We cast them to strings. As a result, the file is bigger (140 KB against 72 KB), the id format is not checked, and joins with dbt tables need a cast. Also, the Spark timestamp has no time zone, so users must know it is in UTC.

One file with dbt, one folder with Spark: which is easier, which scales? One file is easier to read. A folder of files scales better, because each file can be written and read in parallel.

What did this lab use instead of dbt tests? Only Python assert lines and a manual duplicate check. To get the same safety, we would need checks that stop the job: no duplicate ids, no null ids, only accepted values, and every order linked to a user. A tool like Great Expectations or PyDeequ can do this.

What plays the role of ref() and source()? The Python variables and the order of the cells. The developer must run the cells in the right order. There is no global graph of the project like in dbt.

Why can we not compare the build times directly? The two labs do not do the same work: dbt only creates views, while Spark writes the data twice. Spark also needs time to start its JVM and download Iceberg. With such a small dataset, this start time is bigger than the work itself.

dbt with DuckDB or Spark? For 10 GB per day: dbt with DuckDB, because one machine is enough. For 10 TB per day: Spark, because the work must be shared between many machines. For analysts who only know SQL: dbt, because the models are written in SQL.

8. Exercises
1. birthdate_is_valid
python
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))

stg_users_valid = (
    stg_users
    .join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate").cast("timestamp") <= F.col("first_order_at"), F.lit(True)),
    )
    .drop("first_order_at")
)
python
stg_users_valid_sql = spark.sql('''
    SELECT u.*,
           coalesce(CAST(u.birthdate AS TIMESTAMP) <= f.first_order_at, true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (
        SELECT user_id, min(ordered_at) AS first_order_at
        FROM stg_orders
        GROUP BY user_id
    ) f ON u.user_id = f.user_id
''')

Both versions give 45 valid and 5 invalid birthdates, and they agree for all 50 users. This is the same result as the dbt lab. The difference is that dbt also replaced the invalid birthdates with NULL, while here we only add the flag.

2. Python UDF and pandas UDF
python
ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")

def extract_zip(line):
    if line is None:
        return None
    m = ZIP_RE.search(line)
    return m.group(2) if m else ""

zip_udf = F.udf(extract_zip, "string")

@F.pandas_udf("string")
def zip_pandas(s: pd.Series) -> pd.Series:
    extracted = s.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1]
    return extracted.where(s.isna(), extracted.fillna(""))

%%timeit only works in Jupyter, so I used the timeit module. Each version ran 5 times.

Version	Best time	Mean time
regexp_extract()	0.106 s	0.121 s
Python UDF	0.130 s	0.232 s
pandas UDF	0.123 s	0.367 s

The built-in function is the fastest, because it runs inside Spark. The UDFs are slower, because the data has to go to Python and back. The pandas UDF has a high mean because its first run is slow (it starts Python and loads pandas). With only 50 rows, the differences are small. The pandas UDF is only really faster on big data. Conclusion: use built-in functions when possible.

3. Cache
Build of the silver tables	Time
Without cache	0.548 s
With cache on the bronze tables	0.299 s

With the cache, it is about 45% faster, because Spark does not read the CSV files from S3 again. The execution plan now starts with InMemoryTableScan, which means Spark reads the data from memory.

4. Iceberg append and time travel

I added one new order to the Iceberg table and to the Parquet folder.

text
|snapshot_id        |parent_id         |operation|
|675346543749128507 |NULL              |overwrite|
|7007135950881517588|675346543749128507|append   |
Query	Orders
Iceberg table, version before the append	2829
Iceberg table, current version	2830
Parquet folder after the append	2830

The difference: Iceberg keeps a history. Each write creates a new snapshot, and the old files are kept. So we can read the table as it was before, with VERSION AS OF. A Parquet folder has no history: it is just the files present now. There is no way to read the old version.

To keep the silver layer clean for the next labs, I removed the new order. For Iceberg, a rollback to the first snapshot was enough, with no data to recompute. For Parquet, all the data had to be written again. Both are back to 282