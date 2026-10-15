Lab 5: Bronze to Silver transformation with dbt and DuckDB

Maxime CURIE

Environment: Onyxia vscode-pyspark service, dbt Core 1.12.6, dbt-duckdb 1.11.0, S3-compatible backend SeaweedFS.

The dbt project is in lab_medallion/. The DuckDB database, target/ and logs/ are not committed (see the root .gitignore).

1. Project structure
text
lab_medallion/
├── dbt_project.yml            # silver models materialized as views in the silver schema
├── profiles.yml               # DuckDB connection, no credential
├── models/
│   ├── bronze/sources.yml     # bronze.users and bronze.orders, read from S3
│   └── silver/
│       ├── stg_users.sql
│       ├── stg_orders.sql
│       └── silver.yml         # documentation and generic tests
├── seeds/
│   ├── states.csv             # reference data for the state codes
│   └── zip_prefixes.csv       # reference data for exercise 4
└── tests/
    ├── generic/positive.sql   # custom generic test (exercise 2)
    ├── assert_orders_have_users.sql
    ├── assert_orders_after_user_birth.sql
    └── assert_users_zip_matches_state.sql

S3 access goes through the persistent DuckDB secret s3_onyxia_connection, recreated from the default AWS profile at the start of each session (the Onyxia credentials are temporary). The bucket name is read with env_var('LAB_BUCKET_NAME'), so it never appears in the code.

2. Bronze sources

models/bronze/sources.yml declares bronze.users and bronze.orders. With dbt-duckdb, external_location replaces {{ source(...) }} with a read_csv call on the S3 object: dbt reads the bronze files but never modifies them.

3. Silver models
stg_users: renamed columns (uuid to user_id, mail to email), types (UUID, DATE), normalized values, and the 2-line address split into street, city, state, zip_code. Military addresses (DPO AP 09617) have no city: city is NULL.
stg_orders: typed columns (UUID, TIMESTAMP WITH TIME ZONE, INTEGER) and deduplication on order_id with qualify row_number() ... = 1, so a replay of the bronze layer does not create duplicates.

dbt run --select silver creates the views main_silver.stg_users and main_silver.stg_orders. target/compiled/ contains the SQL with the Jinja references resolved, target/run/ the create view statements sent to DuckDB.

4. Data tests

Missing seed in the statement. silver.yml tests state with relationships to ref('states'), but the statement never creates this seed. Without it dbt cannot run the tests. seeds/states.csv was added: the 50 states, the District of Columbia, the territories (PR, GU, VI, ...) and the military codes (AA, AE, AP) used by the Faker addresses.

Results:

13 generic tests: all PASS.
assert_orders_have_users (singular): PASS.
--store-failures writes one table per test in the main_dbt_test__audit schema (14 tables, all empty since the tests pass). Long test names are shortened by dbt.
assert_orders_after_user_birth: FAIL 288. The orders are dated 2020 but Faker generates birthdates relative to the current date. With severity='warn' the pipeline completes with PASS=14 WARN=1 ERROR=0.
5. Full pipeline, lineage and documentation

After deleting the database, dbt build loads the seeds, builds the models and runs their tests following the dependency graph. dbt ls --select +stg_orders returns source:lab_medallion.bronze.orders and lab_medallion.silver.stg_orders. dbt docs generate --static writes a single file, target/static_index.html, with the models, columns, tests and lineage graph.

6. Silver export to Parquet

stg_users and stg_orders were exported with COPY ... TO 's3://<bucket>/silver/dbt/<name>.parquet' (FORMAT parquet). The view and the Parquet file contain the same rows: the view is a logical materialization (the query runs on every read), the Parquet file a physical one (the transformation runs once, at export time).

7. Exercises
1. username_normalized

lower(trim(username)), documented in silver.yml and tested with unique and not_null.

2. Order validation
quantity positive: a custom generic test, tests/generic/positive.sql, reusable on any column (dbt Core has no built-in test for this).
product in the expected list: generic accepted_values test.
every user_id in stg_users: generic relationships test, plus the singular test assert_orders_have_users.
3. Invalid birthdates

stg_users now references stg_orders to get the first order of each user. A birthdate later than the first order is replaced with NULL, and birthdate_is_valid keeps the result of the validation:

birthdate_is_valid	users
true	45
false	5

The severity='warn' was removed from assert_orders_after_user_birth: it passes as a blocking test. The 288 failing rows came from the orders of these 5 users. dbt build: PASS=23 WARN=0 ERROR=0.

4. Invalid ZIP codes

seeds/zip_prefixes.csv maps each state to the ranges of the 3 first digits of its ZIP codes. The singular test assert_users_zip_matches_state (severity warn) returns the users whose ZIP prefix is outside the ranges of their state.

Result: 49 users out of 50 have an inconsistent state/ZIP combination. The generator draws the ZIP code at random, independently of the state, and a state only covers a few percent of the possible prefixes.

Note: the ranges are an approximation built from the public USPS 3-digit prefixes, good enough to detect random combinations but not an authoritative source.

Where should the reference data come from? From an authoritative external source: the USPS ZIP code files or the Census Bureau ZCTA (ZIP Code Tabulation Areas), not a manual list.
Should it be stored as a dbt seed? Yes while it is small and rarely changes: it is versioned in Git, reviewed like code, and loaded with dbt seed. A large or frequently updated reference would be ingested like a bronze source.
Should the validation happen in the silver layer? Yes: silver is where records are validated. The rows should be kept and flagged (for example a zip_is_valid column), not deleted, so that downstream users decide how to handle them.
What happens when the reference data changes? After updating the seed and running dbt build, the test results can change at once. To keep the history, the reference needs validity dates (for example a dbt snapshot).
5. Materialization experiment
	view	table
table_type in information_schema	VIEW	BASE TABLE
Size of lab_medallion.duckdb	780 KB	1.8 MB

The view size includes the 2 seeds and the DuckDB file overhead: the silver models themselves store no data. With tables, the 50 users and 2829 orders are copied into the file, which is more than twice as large.

Where is the data stored? View: nowhere, only the query is stored and the data stays in the CSV files on S3. Table: in the DuckDB file.
When is the transformation executed? View: on every query. Table: once, during dbt build.
What happens when the underlying CSV changes? View: the next query returns the new data. Table: the result stays as it was until the next dbt build.
Which approach requires more storage? The table, which copies the data.
Which approach avoids re-reading the source files for every query? The table.
Questions

The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later? The raw data is still available exactly as it was received. Once the transformation is fixed, dbt build recomputes the silver layer from the bronze files, including the historical data, without asking the producers to send the data again. Bronze is the source of truth that makes the pipeline replayable.

The source reads the whole file on every query. Which format and layout for orders ingested every hour for years? Parquet files (columnar, compressed, with min/max statistics) partitioned by date, for example bronze/orders/dt=YYYY-MM-DD/ or by year and month, each hourly ingestion adding a new file. The queries only read the partitions and columns they need. A table format such as Apache Iceberg or Delta Lake would add transactions, schema evolution and compaction of the small hourly files.

The silver models are views. What happens when a view is queried, and when is it preferable to a table? The query stored in the view is executed: DuckDB reads the CSV files on S3 again and applies the transformation. A view is preferable when the data is small, changes often and must always be up to date, or when storage must be avoided. A table is preferable when the transformation is expensive or the model is queried often.

UUID values are stored in 16 bytes. How many bytes does the VARCHAR representation use? 36 characters (32 hexadecimal digits and 4 hyphens), so 36 bytes in ASCII, more than twice the 16 bytes of the binary UUID type, without counting the overhead of the string storage.

Which strategy for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region? Age: the invalid birthdates must not be used, so they are replaced with NULL (exercise 3) and excluded from the age statistics, with the number of excluded customers displayed. Sales per region: the birthdate is not used, so the orders must all be kept: rejecting them would understate the sales. Keeping the records with a warning test is enough.

The addresses associate states with random zip codes. Which test would detect it, and which reference data would it require? A test checking that the ZIP code belongs to the ranges assigned to the state (exercise 4), with a reference table mapping states to ZIP code prefixes or ranges, from the USPS or the Census Bureau.