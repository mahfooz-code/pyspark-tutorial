#   What is PySpark?
    
    PySpark is an Apache Spark interface in Python.
    It is used for collaborating with Spark using APIs written in Python. 
    It also supports Spark's features like Spark DataFrame, Spark SQL, Spark Streaming, Spark MLlib and Spark Core. 
    It provides an interactive PySpark shell to analyze structured and semi-structured data in a distributed environment.
    PySpark supports reading data from multiple sources and different formats. 
    It also facilitates the use of RDDs (Resilient Distributed Datasets).
    PySpark features are implemented in the py4j library in python.

#   What are the main characteristics of (Py)Spark?
    
    Some of  the main characteristics of (Py)Spark are:
    
    Here Nodes are abstracted that says no possible to address an individual node.
    Also, Network is abstracted, that means there is only implicit communication possible.
    Moreover, it is based on Map-Reduce, that means programmer provides a map and a reduce function here.
    And, PySpark is one of the API for Spark.

#   Pros of PySpark?
    
    Some of the benefits of using PySpark are:
    
    For simple problems, it is very simple to write parallelized code.
    Also, it handles Synchronization points as well as errors.
    Moreover, in Spark, many useful algorithms is already implemented.
 

#   Cons of PySpark?
    Some of the limitations on using PySpark are:
    
    It is difficult to express a problem in MapReduce fashion sometimes.
    Also, Sometimes, it is not as efficient as other programming models.

#   Prerequisites to learn PySpark?
    
    It is being assumed that the readers are already aware of what a programming language and a framework is, before proceeding with the various concepts given in this tutorial. 
    Also, if the readers have some knowledge of Spark and Python in advance, it will be very helpful.

#   What do you mean by PySpark SparkContext?
    
    In simple words, an entry point to any spark functionality is what we call SparkContext.
    While it comes to PySpark, SparkContext uses Py4J(library) in order to launch a JVM.
    In this way, it creates a JavaSparkContext.
    However, PySpark has SparkContext available as 'sc', by default.

#   Explain PySpark SparkConf?
    
    Mainly, we use SparkConf because we need to set a few configurations and parameters to run a Spark application on the 
    local/cluster.
    In other words, SparkConf offers configurations to run a Spark application.

#   CAN YOU EXPLAIN THE MAIN FEATURES OF SPARK APACHE?
    
    Supports several programming languages
    
    Spark can be coded in four programming languages, i.e. Java, Python, R, and Scala.
    It also offers high-level APIs for them. Additionally, Apache Spark supplies Python and Scala shells. 
    
    Lazy Evaluation
    
    Apache Spark uses the principle of lazy evaluation to postpone the evaluation before it becomes completely mandatory.
    
    Machine Learning
    
    The MLib machine learning component of Apache Spark is useful for extensive data processing.
    It removes the need for different engines for processing and machine learning.
    
    Modern Format Assistance
    
    Apache Spark supports multiple data sources, like  Cassandra, Hive, JSON, and Parquet.
    The Data Sources API provides a pluggable framework for accessing structured data through Spark SQL. 
    
    Real-Time Computation
    
    Spark is specifically developed to satisfy massive scalability criteria.
    Thanks to in-memory computing, Spark’s computing is real-time and has less delay.
    
    Speed
    
    Spark is up to 100x faster than Hadoop MapReduce for large-scale data processing.
    Apache Spark is capable of achieving this incredible speed by optimized  portioning.
    The general-purpose cluster-computer architecture handles data across partitions that parallel distributed data processing with limited network traffic.
    
    Hadoop Integration

    Spark provides seamless access to Hadoop and is a possible substitute for the Hadoop MapReduce functions.
    Spark is capable of operating on top of the existing Hadoop cluster using YARN for scheduling resources.


#   DEFINE SPARK.
Spark is a parallel system for data analysis. It allows a quick, streamlined big data framework to integrate batch, streaming, and immersive analytics.

54. WHY USE SPARK?
Spark is a 3rd gen distributed data processing platform. It’s a centralized big data approach for big data processing challenges such as batch, interactive or streaming processing. It can ease a lot of big data issues.

55. WHAT IS RDD?
The primary central abstraction of Spark is called Resilient Distributed Datasets. Resilient Distributed Datasets are a set of partitioned data that fulfills these characteristics. The popular RDD properties are immutable, distributed, lazily evaluated, and catchable.

56. THROW SOME LIGHT ON WHAT IS IMMUTABLE.
If a value has been generated and assigned, it cannot be changed. This attribute is called immutability. Spark is immutable by nature. It does not accept upgrades or alterations. Please notice that data storage is not immutable, but the data content is immutable.

57. HOW CAN RDD SPREAD DATA?
RDD can dynamically spread data through various parallel computing nodes.

58. WHAT ARE THE DIFFERENT ECOSYSTEMS OF SPARK?
Some typical Spark ecosystems are:

Spark SQL for developers of SQL
Spark Streaming for data streaming
MLLib for algorithms of machine learning
GraphX for computing of graph
SparkR to work on the Spark engine
BlinkDB, which enables dynamic queries of large data
GraphX, SparkR, and BlinkDB are in their incubation phase.

59. WHAT ARE PARTITIONS?
Partition is a logical partition of records, an idea taken from Map-reduce (split) in which logical data is directly obtained to process data. Small bits of data can also help in scalability and fasten the operation. Input data, output data & intermediate data are all partitioned RDDs.

60. HOW DOES SPARK PARTITION DATA?
Spark uses the map-reduce API for the data partition. One may construct several partitions in the input format. HDFS block size is partition size (for optimum performance), but it’s possible to adjust partition sizes like Split.

61. HOW DOES SPARK STORE DATA?
Spark is a computing machine without a storage engine in place. It can recover data from any storage engine, such as HDFS, S3, and other data services.

62. IS IT OBLIGATORY TO LAUNCH THE HADOOP PROGRAM TO RUN A SPARK?
It is not obligatory, but there is no special storage in Spark. Thus you must use the local file system to store the files. You may load and process data from a local device. Hadoop or HDFS is not needed to run a Spark program.

63. WHAT’S SPARKCONTEXT?
When the programmer generates RDDs, SparkContext connects to the Spark cluster to develop a new SparkContext object. SparkContext tells Spark to navigate the cluster. SparkConf is the central element for creating an application for the programmer.

64. HOW IS SPARKSQL DIFFERENT FROM HQL AND SQL?
SparkSQL is a special part of the SparkCore engine that supports SQL and HiveQueryLanguage without modifying syntax. You will enter the SQL table and the HQL table.

65. WHEN IS SPARK STREAMING USED?
It is an API used for streaming data and processing it in real-time. Spark streaming collects streaming data from various services, such as web server log files, data from social media, stock exchange data, or Hadoop ecosystems such as Kafka or Flume.

66. HOW DOES THE SPARK STREAMING API WORK?
The programmer needs to set a specific time in the setup, during which the data that goes into the Spark is separated into batches. The input stream (DStream) goes into the Spark stream. 

The framework splits into little pieces called batches, then feeds into the Spark engine for processing. The Spark Streaming API sends the batches to the central engine. Core engines can produce final results in the form of streaming batches. Production is in the form of batches, too. It allows the streaming of data and batch data for processing.

67. WHAT IS GRAPHX?
GraphX is a Spark API for editing graphics and arrays. It unifies ETL, analysis, and iterative graph computing. Its fastest graphics system offers error tolerance and easy use without the need for special expertise.

68. WHAT IS FILE SYSTEM API?
The File System API can read data from various storage devices, such as HDFS, S3, or Local FileSystem. Spark utilizes the FS API to read data from multiple storage engines.

69. WHY ARE PARTITIONS IMMUTABLE?
Each transformation creates a new partition. Partitions use the HDFS API such that the partition is immutable, distributed, and error-tolerant. Partitions are, therefore, conscious of the location of the results.

70. DISCUSS WHAT IS FLATMAP AND MAP IN SPARK.
A map is a simple line or row to process the data. Each input object can be mapped to various output items in FlatMap (so the function should return a Seq rather than a unitary item). So most often, it is used to return the Array components.

71. DEFINE BROADCAST VARIABLES.
Broadcast variables allow the programmer to have a read-only variable cached on each computer instead of sending a copy of it with tasks. Spark embraces two kinds of mutual variables: broadcast variables and accumulators. Broadcast variables are stored as Array Buffers, which deliver read-only values to the working nodes.

72. WHAT ARE SPARK ACCUMULATORS IN CONTEXT TO HADOOP?
Off-line Spark debuggers are called accumulators. Spark accumulators are equivalent to Hadoop counters and can count the number of activities. Only the driver program can read the value of the accumulator, not the tasks.

73. WHEN CAN APACHE SPARK BE USED? WHAT ARE THE ADVANTAGES OF SPARK OVER MAPREDUCE?
Spark is quite fast. Programs run up to 100x faster than Hadoop MapReduce in memory. It appropriately uses RAM to achieve quicker performance. 

In Map Reduce Paradigm, you write many Map-reduce tasks and then link these tasks together using the Oozie/shell script. This process is time-intensive, and the role of map-reducing has a high latency.

Frequently, converting production from one MR job to another MR job can entail writing another code since Oozie might not be enough.

In Spark, you can do anything using a single application/console and get the output instantly. Switching between ‘Running something on a cluster’ and ‘doing something locally’ is pretty simple and straightforward. All this leads to a lower background transition for the creator and increased efficiency.

Spark sort of equals MapReduce and Oozie when put in conjunction.

The above-mentioned Spark Scala interview questions are pretty popular and are a compulsory read before you go for an interview.

74. IS THERE A POINT OF MAPREDUCE LEARNING?
Yes. It serves the following purposes:

MapReduce is a paradigm put to use by several big data tools, including Spark. So learning the MapReduce model and transforming a problem into a sequence of MR tasks is critical.
When data expands beyond what can fit into the cluster memory, the Hadoop Map-Reduce model becomes very important.
Almost every other tool, such as Hive or Pig, transforms the query to MapReduce phases. If you grasp the Mapreduce, you would be better able to refine your queries.
75. WHAT ARE THE DRAWBACKS OF SPARK?
Spark uses memory. The developer needs to be cautious about this. Casual developers can make the following mistakes:

It might end up running everything on the local node instead of spreading work to the cluster.
It could reach some web services too many times by using multiple clusters.
The first dilemma is well addressed by the Hadoop Map reduce model.
A second error is also possible in Map-Reduce. When writing Map-Reduce, the user can touch the service from the inside of the map() or reduce() too often. This server overload is also likely when using Spark.
