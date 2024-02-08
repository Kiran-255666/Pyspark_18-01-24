
<!-- README.md was wriiten in beautiful MacDown  -->
# Using Spark Streaming in Azure Databricks



<!-- badges: start -->
![](http://img.shields.io/badge/Azure-Databricks-red.svg)

<!-- badges: end -->




<!-- wp:paragraph -->
<p>Spark Streaming is the process that can analyse not only batches of data but also streams of data in near real-time. It gives the powerful interactive and analytical applications across both hot and cold data (streaming data and historical data). Spark Streaming is a fault tolerance system, meaning due to lineage of operations, Spark will always remember where you stopped and in case of a worker error, another worker can always recreate all the data transformation from partitioned RDD (assuming that all the RDD transformations are deterministic).</p>
<!-- /wp:paragraph -->

<div>
<p>

</p>
</div>


<!-- wp:paragraph -->
<p>Spark streaming has a native connectors to many data sources, such as HDFS, Kafka, S3, Kinesis and even Twitter.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Start your Workspace in Azure Databricks. Create new notebook, name it: <em>Day23_streaming</em> and use the default language: <em>Python</em>. If you decide to use EventHubs from reading data from HDFS or other places, Scala language might be slightly better.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>If you will be using Spark context, otherwise just import pyspark.sql namespace.</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">
from pyspark.sql.types import *
from pyspark.sql.functions import *</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>We will be using the demo data from databricks-datasets folder:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">%fs ls /databricks-datasets/structured-streaming/events/</pre>
<!-- /wp:syntaxhighlighter/code -->



<!-- wp:paragraph -->
<p>And you can check the structure of one file, by using:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">%fs head /databricks-datasets/structured-streaming/events/file-0.json</pre>
<!-- /wp:syntaxhighlighter/code -->



<!-- wp:paragraph -->
<p>You must do the  initialisation of the stream with:</p>
<!-- /wp:paragraph -->

<!-- wp:list -->
<ul><li>inputPath (where your files will be coming)</li><li>Schema of the input files</li><li>ReadStream function with a function if schema, input data additional options (As: picking one file at a time)</li><li>Aggregate function (for count of events in this particular case) and use of ReadStream function.</li></ul>
<!-- /wp:list -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">inputPath = "/databricks-datasets/structured-streaming/events/"

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as stream of one at a time
    .json(inputPath)
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>You start a streaming computation by defining a sink and starting it. In this case, to query the counts interactively, set the <em>complete</em>set of 1 hour counts to be in an in-memory table.</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>Run the following command to examine the outcome of a query.</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)</pre>
<!-- /wp:syntaxhighlighter/code -->

<!-- wp:paragraph -->
<p>And once the cluster is running, you can do variety of analysis.  The Key component is the ".start" method - embedded in the main function</p>
<!-- /wp:paragraph -->

<!-- wp:paragraph -->
<p>You can also further shape the data by using Spark SQL:</p>
<!-- /wp:paragraph -->

<!-- wp:syntaxhighlighter/code -->
<pre class="wp-block-syntaxhighlighter-code">%sql 
SELECT 
 action
,date_format(window.end, "MMM-dd HH:mm") as time
,count 
FROM counts 
ORDER BY time, action</pre>
<!-- /wp:syntaxhighlighter/code -->







