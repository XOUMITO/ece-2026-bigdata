Lab 6: Introduction to Spark's RDD API

Maxime CURIE

Environment: Onyxia vscode-pyspark service, PySpark shell in local[*] mode, Python 3.13.

The two bronze files were downloaded from S3 to this folder (users.csv, orders.csv), because RDDs read local files. They are not committed (see .gitignore) and were deleted at the end.

1. RDD creation and partitions
python
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print(users_lines.getNumPartitions(), orders_lines.getNumPartitions())   # 2 2
print(orders_lines.repartition(8).getNumPartitions())                    # 8

What decides the default number of partitions of textFile()? The minPartitions parameter, which is min(defaultParallelism, 2) = 2 by default, and the split size (128 MB). Both files are much smaller than one split, so Spark just cuts each file into 2 partitions. A large file would get about one partition per 128 MB.

repartition() or coalesce(): which one shuffles? repartition() always shuffles: it redistributes all the rows to get balanced partitions, so it can increase the number of partitions. coalesce() does not shuffle by default: it merges existing partitions, so it can only decrease the number of partitions, and the result can be unbalanced.

2. Lazy evaluation and lineage
python
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
text
(2) PythonRDD[10] at RDD at PythonRDD.scala:58 []
 |  orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
 |  orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []

This is only a plan: no line was filtered yet. orders_data.count() runs it and returns 2829.

A second count(): is there a new stage? Why? Yes. A new job with a new stage is created, because nothing is kept in memory between two actions: Spark reads the file and filters it again from the beginning.

Where would .cache() change this? On orders_data, before the first count(). The first count would compute the partitions and keep them in memory. The second count would still be a new job, but it would read the cached partitions instead of the file.

3. The multiline address problem
text
uuid,username,name,sex,address,mail,birthdate
bdd640fb-0667-4ad1-9c80-317fa3b1799d,garzaanthony,Charles Garcia,M,"908 Jennifer Squares
Robinsonshire, KY 01352",helenpeterson@gmail.com,1935-10-06

users_lines.count() returns 101 (1 header + 50 users x 2 lines). Keeping only the lines that start with a UUID gives 50 users. But the end of the address, the email and the birthdate are on the deleted lines, so they are lost.

Why does spark.read.csv(multiLine=True) not have this problem? It understands the CSV syntax: a line break inside quotes is part of the value, not the end of the record. textFile() splits on every \n and knows nothing about quotes or columns.

When is a line-oriented reader like textFile() safe? When one record is exactly one physical line, so no value contains a line break (or line breaks are escaped). Examples: JSON Lines, log files, CSV files without multiline fields. This also lets Spark split the file anywhere and read it in parallel.

4. Manual parsing of orders
python
def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {"order_id": order_id, "user_id": user_id, "date": date,
            "quantity": int(quantity), "product": product.strip().lower()}

orders_rdd = orders_data.map(parse_order).cache()

What if one line had an extra comma inside product? split(",") would return 6 parts instead of 5, and the unpacking would raise ValueError: too many values to unpack. Because map is lazy, the error would only appear at the first action that reaches this line, and the whole job would fail.

Where does the DataFrame API do this check, and when? spark.read.csv applies a schema (given or inferred) and understands quotes. Bad values are handled by the read mode: PERMISSIVE (set to null), DROPMALFORMED (drop the row) or FAILFAST (fail the job). Column names and types are also checked when the query plan is built, before any action. In exercise 3, groupBy("user_id") failed at once with UNRESOLVED_COLUMN (the CSV column is user_uuid), without reading any data.

5. Total quantity per user: reduceByKey vs groupByKey
python
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
user_quantity_grouped = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)

Both give the same totals, for example ('f143262f-...', 12), ('969b6662-...', 40), ('ce9e1a11-...', 253).

What does groupByKey shuffle, and why is it more expensive? It sends every (user_id, quantity) pair through the shuffle (2829 values), then builds the full list of values for each key. reduceByKey first adds the values inside each partition, so it only sends one partial sum per user and per partition (at most 50 x 2 values). groupByKey moves more data over the network and must keep all the values of a key in memory.

When is groupByKey unavoidable? When you need all the values of a key at the same time, and the result cannot be built by merging partial results with an associative function. Examples: an exact median or percentile, sorting the orders of a user to compute the time between two orders, or building the full list of values.

6. Manual deduplication
python
def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b

orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()

The generator creates no duplicate order_id, so no order is removed. Comparing the dates as strings works because the ISO format sorts in time order when all dates use the same time zone.

What plays the role of PARTITION BY order_id? The shuffle of reduceByKey: it sends all the rows with the same order_id to the same partition, then applies most_recent two by two.

Which version states the intent more directly? The window function: row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) = 1 says "keep the most recent row per order". With reduceByKey, you must read most_recent to understand the goal, and write the comparison logic yourself.

7. Joining orders with users
python
def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username

users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)

Each element is (user_id, (order, username)), for example ('f143262f-...', ({... 'product': 'bread'}, 'kayla51')). join() and leftOuterJoin() both return 2829 rows, with 0 order without a user.

How could Spark skip the shuffle of the join? Both RDDs must be co-partitioned: partitioned by the same key, with the same partitioner and the same number of partitions (for example partitionBy(4) on both, then persisted). Matching keys are then already on the same partition. Another option is to broadcast the small RDD to all the executors.

What changes with leftOuterJoin()? An order whose user_id has no matching user is kept, with None as username: (user_id, (order, None)). With join(), it disappears without any warning.

What is the risk of .collect() on enriched? collect() brings all the rows to the driver, a single process with limited memory. On a large dataset, this can crash the driver with an out of memory error. It is better to filter or aggregate first, then use take(n) or write the result to files.

8. Exercises
1. aggregateByKey
python
user_stats = (orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey((0, 0),
                    lambda acc, q: (acc[0] + 1, acc[1] + q),
                    lambda a, b: (a[0] + b[0], a[1] + b[1])))
user_avg = user_stats.mapValues(lambda c: round(c[1] / c[0], 2))
user_id	(order_count, total_quantity)	average quantity
f143262f-...	(4, 12)	3.0
969b6662-...	(12, 40)	3.33
ce9e1a11-...	(85, 253)	2.98

Check: the sums of all counts and quantities are 2829 and 8615, the total number of orders and the total quantity. The count and the sum are computed in one pass and one shuffle.

2. An order without user
python
fake = sc.parallelize([{"order_id": "fake-order-1", "user_id": "00000000-0000-0000-0000-000000000000",
                        "date": "2020-04-28 00:00:00+00:00", "quantity": 1, "product": "bread"}])
orders_with_fake = orders_rdd.union(fake)
orphans = (orders_with_fake.map(lambda o: (o["user_id"], o))
           .leftOuterJoin(users_kv)
           .filter(lambda kv: kv[1][1] is None))

Result: 2830 orders, 1 orphan: the fake order, with None as username.

3. The same aggregation with the DataFrame API
python
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True).withColumnRenamed("user_uuid", "user_id")
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
The CSV column is user_uuid, so it is renamed first. Without this, Spark failed at once with UNRESOLVED_COLUMN, before reading any data.
Same result as the RDD version: comparing both as dictionaries gives 50 50 True.
Line count: 2 lines for the DataFrame (header and types included), about 6 for the RDD (header filter, parse_order, map, reduceByKey).

.explain() (final plan, read from bottom to top):

text
*(2) HashAggregate(keys=[user_id], functions=[sum(quantity)])
+- AQEShuffleRead coalesced
   +- Exchange hashpartitioning(user_id, 200)
      +- *(1) HashAggregate(keys=[user_id], functions=[partial_sum(quantity)])
         +- *(1) Project [user_uuid AS user_id, quantity]
            +- FileScan csv ... ReadSchema: struct<user_uuid:string,quantity:int>

.toDebugString() of the RDD version:

text
(2) PythonRDD[100] at collect
 |  MapPartitionsRDD[21] at mapPartitions
 |  ShuffledRDD[20] at partitionBy
 +-(2) PairwiseRDD[19] at reduceByKey
    |  PythonRDD[18] at reduceByKey
    |  PythonRDD[16] at RDD
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[3] at textFile
    |  orders.csv HadoopRDD[2] at textFile

What the DataFrame API gives for free:

Column pruning: only 2 columns out of 5 are read (ReadSchema).
Partial aggregation: partial_sum before the shuffle, the same strategy as reduceByKey, chosen automatically by the Catalyst optimizer.
Adaptive Query Execution: the shuffle uses 200 partitions by default, and AQE merged them at run time (AQEShuffleRead coalesced) because there are only 50 users.
Code generation: *(1) and *(2) mean the stages are compiled to Java code. The RDD version calls Python lambdas row by row.

The RDD lineage only shows how data moves between RDDs (the +-(2) marks the shuffle, so a new stage). It has no column, no type, and Spark cannot optimize the Python lambdas.

4. The effect of .cache()

Each block was run twice. The first run includes warm-up time (starting the Python workers).

	orders_rdd.count()	user_quantity.count()
With cache, 1st run	0.318 s	0.181 s
With cache, 2nd run	0.106 s	0.160 s
Without cache, 1st run	0.137 s	0.294 s
Without cache, 2nd run	0.266 s	0.208 s
With cache, the parsed orders stay in memory: CachedPartitions: 2; MemorySize: 168.0 KiB, also visible in the Storage tab of the Spark UI. After unpersist(), the cache is empty.
Warm, the version without cache is slower (0.266 s vs 0.106 s for orders_rdd.count()), because each action reads the file and parses it in Python again.
The measures are noisy (0.137 s then 0.266 s for the same action). With only 330 KB of data, the fixed cost of a Spark job is as large as the real work. Several runs and a median would be needed for a precise result.
The gap is smaller for user_quantity because Spark reuses the shuffle files written by a previous run (stages marked as skipped in the Jobs tab).
Cache is useful when an RDD that is expensive to compute is reused by several actions on a large dataset. It uses memory, so it should be released with unpersist() when it is no longer needed.