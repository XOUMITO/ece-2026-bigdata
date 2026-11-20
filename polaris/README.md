Polaris labs: deployment and governance of Iceberg tables

Maxime CURIE

Environment: Onyxia vscode-pyspark service, Apache Polaris 1.8.0 deployed with Helm in my namespace, PostgreSQL for the Polaris database, PySpark shell in local mode, Spark 4.1.1, Iceberg runtime iceberg-spark-runtime-4.1_2.13:1.11.0, S3 storage on SeaweedFS.

This README covers two labs: the deployment of Polaris (lab 1) and the governance of the Iceberg tables with Polaris (lab 2).

Lab 1: Deploy Polaris with Helm
1. Resources of the service

The vscode-pyspark StatefulSet was patched with kubectl patch so that the service gets requests of 50m CPU and 200Mi of memory, and limits of 2 CPU and 2500Mi. Small requests leave room in the namespace quota for Polaris and PostgreSQL.

2. Configuration files

The files are in polaris/deployment/ and are committed:

text
polaris/deployment/
├── Chart.yaml                      Polaris 1.8.0 and PostgreSQL charts as dependencies
├── Chart.lock
├── values.yaml                     database, resources, S3 credentials, init container
└── templates/
    ├── bootstrap-job.yaml          creates the catalog, principals and roles
    └── polaris-db-secret.yaml      database secret, password generated automatically

The downloaded charts (polaris/deployment/charts/) are not committed. This folder was added to .gitignore.

3. ConfigMap and secrets
polaris-config (ConfigMap): catalog name, Polaris URIs, realm POLARIS, base location s3://user-m-curie-ece/warehouse/, S3 endpoint.
polaris-credential (Secret): the secret of the root principal.
polaris-s3 (Secret): the S3 credentials that Polaris uses to read the table metadata in the bucket.
4. Deployment

Polaris was installed with helm upgrade in install mode, and the health endpoint http://polaris-mgmt:8182/q/health returned "status": "UP".

The bootstrap Job created the RBAC model of the lab:

Principal	Principal role	Catalog role	Privileges
dev	data_engineer	catalog_contributor	CATALOG_MANAGE_ACCESS, CATALOG_MANAGE_METADATA, CATALOG_MANAGE_CONTENT
analyst	data_analyst	catalog_reader	TABLE_READ_DATA, TABLE_READ_PROPERTIES, CATALOG_READ_PROPERTIES

The catalog is called user-m-curie-ece, like my bucket.

5. Credentials of the principals

The credentials of dev and analyst were read from the logs of the bootstrap Job and written to /home/onyxia/.polaris-credentials.env. This file is outside the repository and is never committed. Polaris shows these credentials only once, so I also saved a copy on my computer.

Notes
The S3 credentials of Onyxia are temporary (about 12 hours). When they expire, Polaris can no longer read the metadata. The fix is to recreate the polaris-s3 secret with new credentials and restart the Polaris deployment.
My first git commands were run from ~/work, which is not a repository. They failed, and I ran them again from the project folder.
Lab 2: Govern Iceberg tables with Polaris
1. Spark session with two catalogs

The session declares two Spark catalogs:

lab: the Hadoop catalog of the Spark lab. Spark reads the table pointers directly in the bucket.
polaris: a REST catalog. Spark asks Polaris for the tables, with the credential of the principal chosen in POLARIS_PRINCIPAL.

Changes for my environment:

The S3 credentials were exported from the AWS profile before starting PySpark.
The PySpark shell already creates a session, so I ran spark.stop() before building the new one. Otherwise Spark keeps the old session and ignores the catalog settings. When this happens, a query on polaris.silver fails with spark_catalog requires a single-part namespace.
The long session cell was cut when pasted into the shell. I split it into five short blocks that add the settings one by one to the same builder. The settings are exactly those of the lab.
To change the principal, I closed the shell with exit() and started a new session, instead of restarting a notebook kernel.
Questions

Which component answered each SHOW NAMESPACES command? lab was answered by Spark itself: the Hadoop catalog lists the folders under warehouse/ in the bucket, so it showed silver. polaris was answered by the Polaris server through its REST API. It returned an empty list, even though the silver folder exists in the bucket. This shows that Polaris does not look at the bucket: it answers from its own database.

Where is the credential used? In a request to Polaris. Spark sends the client id and secret of the principal to the token endpoint of Polaris, and gets an access token. Then it sends this token with each REST call. The credential is not used for the bucket: there is no credential vending here, so Spark reads and writes the files with its own S3 keys.

2. Register the silver table

The current metadata file of lab.silver.orders was v3.metadata.json, not v1, because of the append and the rollback done in the Spark lab (exercise 4).

python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql(f"""
    CALL polaris.system.register_table(
      table => 'silver.orders',
      metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v3.metadata.json'
    )
""").show()

The table returned 2829 orders, like in the Spark lab. The lab statement says 5,000, but my dataset has 2829 orders.

text
|snapshot_id        |operation|committed_at           |
|675346543749128507 |overwrite|2026-11-20 13:36:42.137|
|7007135950881517588|append   |2026-11-20 13:56:40.168|

The two snapshots come from the Spark lab. The current one is 675346543749128507, after the rollback.

Questions

What did register_table write? In Polaris: one new table entry, silver.orders, which points to v3.metadata.json. This entry is stored in the PostgreSQL database of Polaris. In the bucket: nothing. The number of objects under warehouse/silver/orders/ was the same before and after. No data was copied.

Why is it dangerous to keep writing through lab? The table now has two pointers to its current version: version-hint.text for lab, and the Polaris entry for polaris. A write through lab would create v4.metadata.json and move only the lab pointer. Polaris would still point to v3, so its users would not see the new data. A later write through Polaris would start again from v3, and the two histories would split. One of them would be lost from the point of view of the other catalog.

3. Create a table as dev
python
stg_users = spark.table("lab.silver.users")
stg_users.writeTo("polaris.silver.users").createOrReplace()

The table returned 50 users. Its metadata files are named 00000-<uuid>.metadata.json, while the Hadoop catalog names them v1.metadata.json.

Questions

Where did Polaris store the files of silver.users, and why? In s3://user-m-curie-ece/warehouse/silver/users/, with a data/ and a metadata/ folder. Polaris builds the location from the default base location of the catalog (s3://user-m-curie-ece/warehouse/), then the namespace, then the table name. This is the same folder as the users table of the Hadoop catalog. The two tables only differ by the names of their metadata files.

What is the difference between "registered" and "created through Polaris"? In both cases, the files are in my bucket and Spark writes them with its own S3 keys. For the registered table, the files were written before, through the lab catalog, and Polaris only received a pointer to an existing metadata file. For the created table, Polaris chose the location and has managed the pointer since the first commit. All its versions go through Polaris.

4. Schema change as dev
python
spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")

DESCRIBE shows 11 columns, with country at the end.

Questions

Which metadata file records the change? A new file of the users table, 00001-f137f2a3-...metadata.json. It is not the file that register_table pointed to: that one was v3.metadata.json of the orders table. The 00000 file of users is not changed either. Each change writes a new metadata file.

Which privilege does Polaris check? TABLE_WRITE_PROPERTIES, because a schema change updates the table metadata. dev has it through CATALOG_MANAGE_CONTENT.

5. Read as the analyst

With POLARIS_PRINCIPAL = "analyst", the query that counts the orders per product works. The writes are refused:

text
ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]'
and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT

The ALTER TABLE ... ADD COLUMN phone was refused in the same way, for the operation ADD_TABLE_SCHEMA.

Questions

Which privilege is missing for the INSERT? TABLE_WRITE_DATA, which allows adding a snapshot. catalog_reader only gives TABLE_READ_DATA, TABLE_READ_PROPERTIES and CATALOG_READ_PROPERTIES. It has no write privilege at all.

Did the failed INSERT create a snapshot? No. As dev, silver.orders still has the same 2 snapshots and 2829 rows. But Spark had already written a data file in the bucket before asking Polaris to commit. When the commit was refused, Spark aborted the write and cleaned up this file. This shows the limit of Polaris here: it controls the catalog, not the storage.

6. What Polaris enforces, and what it cannot

Polaris checks every catalog operation: list the namespaces and tables, load a table, add a snapshot, change a schema. A refused operation never becomes a new version of the table.

But in this setup, Spark uses its own S3 keys for the files, and there is no credential vending. Anyone with the S3 keys of the bucket can read the Parquet files of any table directly by their path, or write files in the bucket, without asking Polaris. To protect the data itself, Polaris would need to give each principal short-lived S3 credentials limited to the allowed tables. That is credential vending, which needs STS on the storage side.

7. Commit of the metadata

The warehouse/ folder was synchronised from S3 to the repository. The first sync skipped version-hint.text because its size did not change. A second sync that compares the timestamps fixed it, and the file now contains 3.

Commit ffed8f1 adds 9 files: v2 and v3 of orders with their Avro files, the new version-hint.text, and the 00000 and 00001 metadata files of users with their Avro files. Only the metadata is committed, never the data files or the credentials.

Exercises
1. Remove the country column
python
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
The table has its 10 original columns again.
There is still only one snapshot, 4619106804781001842, the one of the table creation.
The metadata log now has three files: 00000 (creation), 00001 (add country) and 00002-578525c1-... (drop country).

This is a metadata operation because Iceberg identifies columns by an id, not by their position in the files. To drop a column, Iceberg only writes a new schema, without this id, in a new metadata file. The Parquet files are not rewritten. They still contain no country values, and a reader simply ignores the columns that are not in the current schema. No data changed, so no snapshot was created.

2. A silver_reader role limited to the silver namespace

First, as dev, I created the gold namespace and a table in it:

python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.table("polaris.silver.users").writeTo("polaris.gold.users").createOrReplace()

Then, with the REST API of Polaris and a token of the root principal, I created a new chain:

Principal	Principal role	Catalog role	Privileges
silver_user	silver_analyst	silver_reader	NAMESPACE_READ_PROPERTIES, TABLE_LIST, TABLE_READ_PROPERTIES, TABLE_READ_DATA on the silver namespace

The grants have the type namespace on ["silver"], while the grants of catalog_reader have the type catalog. That is the only difference, and it limits the scope of the role. The credentials of silver_user were written directly to the credentials file, without being displayed.

A new principal was needed to test cleanly. If silver_reader was given to analyst, the grants of catalog_reader would still allow reading gold.

Results with a Spark session as silver_user:

Query	Result
SELECT count(*) FROM polaris.silver.orders	2829
SELECT count(*) FROM polaris.silver.users	50
SHOW TABLES IN polaris.gold	refused, op LIST_TABLES
SELECT count(*) FROM polaris.gold.users	refused, op LOAD_TABLE
text
ForbiddenException: Forbidden: Principal 'silver_user' with activated PrincipalRoles '[silver_analyst]'
and activated grants via '[silver_analyst, silver_reader]' is not authorized for op LOAD_TABLE

silver_user can read silver but not gold. Polaris refuses even to list the tables of gold, before Spark reads a single file.

Cleanup

As dev:

python
spark.sql("DROP TABLE IF EXISTS polaris.gold.users")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")

The gold table had to be dropped before its namespace, because only an empty namespace can be dropped. At the end, the Polaris catalog only has the silver namespace with the orders table. silver.orders was kept registered, because the Trino lab reads it. A DROP TABLE without PURGE only removes the table from the catalog: its files stay in the bucket.

Polaris itself stays deployed for the next module.