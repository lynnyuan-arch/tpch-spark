# tpch-spark

TPC-H queries implemented in Spark using the DataFrames API.
Tested under Spark 2.4.0

Savvas Savvides

savvas@purdue.edu


### Generating tables

Under the dbgen directory do:
```
make
```

This should generate an executable called `dbgen`
```
./dbgen -h
```

gives you the various options for generating the tables. The simplest case is running:
```
./dbgen
```
which generates tables with extension `.tbl` with scale 1 (default) for a total of rougly 1GB size across all tables. For different size tables you can use the `-s` option:
```
./dbgen -s 10
```
will generate roughly 10GB of input data.

You can then either upload your data to hdfs or read them locally.

### Running

First compile using:

```
sbt package
```

Make sure you set the INPUT_DIR and OUTPUT_DIR in `TpchQuery` class before compiling to point to the
location the of the input data and where the output should be saved.

You can then run a query using:

```
spark-submit --class "com.github.tpch.TpchQuery" --master MASTER target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ##

bin/spark-submit --class "com.github.tpch.TpchQuery" --conf spark.network.timeout=10000000 --conf spark.executor.heartbeatInterval=10000000 --conf spark.executor.memory=32G --master spark://node24:7077 --jars plugin/znspark-1.0-SNAPSHOT.jar tpch/tpch-spark-0.1-SNAPSHOT.jar
bin/spark-submit --class "com.github.tpch.TpchQuery" --conf spark.executor.memory=32G --conf spark.driver.memory=4G --master spark://node24:7077 --jars plugin/znspark-1.0-SNAPSHOT.jar tpch/tpch-spark-0.1-SNAPSHOT.jar

bin/spark-submit --class "com.github.tpch.TpchQuery" --conf spark.executor.memory=32G --conf spark.driver.memory=4G --conf spark.sql.shuffle.partitions=20 --master spark://node24:7077 --jars plugin/znspark-1.0-SNAPSHOT.jar tpch/tpch-spark-0.1-SNAPSHOT.jar

bin/spark-submit --class "com.github.tpch.TpchQuery" \
--conf spark.submit.deployMode=cluster \
--conf spark.network.timeout=10000000 \
--conf spark.executor.heartbeatInterval=10000000 \
--conf spark.executor.memory=256G \
--conf spark.driver.memory=128G \
--master spark://node24:7077 \
--jars plugin/znspark-1.0-SNAPSHOT.jar \
tpch/tpch-spark-0.1-SNAPSHOT.jar

```

where ## is the number of the query to run e.g 1, 2, ..., 22
and MASTER specifies the spark-mode e.g local, yarn, standalone etc...



### Other Implementations

1. Data generator (http://www.tpc.org/tpch/)

2. TPC-H for Hive (https://issues.apache.org/jira/browse/hive-600)

3. TPC-H for PIG (https://github.com/ssavvides/tpch-pig)
