#   Lineage chart
    Lineage map reports to the graph for the RDD parent as a whole.

#   EXPLAIN THE IDLE ASSESSMENT IN SPARK.


#   EXPLAIN THE ADVANTAGE OF A LAZY EVALUATION.


#   WHAT IS THE MAP-REDUCE LEARNING FUNCTION?
    Map Reduce is a model used for a vast amount of data design.

#   WHEN PROCESSING INFORMATION FROM HDFS, IS THE CODE PERFORMED NEAR THE DATA?

#   WHERE DOES THE SPARK DRIVER OPERATE ON YARN?
    The Spark driver operates on the client computer.

#   EXPLAIN THE SPARK EXECUTOR.
    Executors are worker nodes processes in charge of running individual tasks in a given Spark job.

#    EXPLAIN THE MEANING OF A WORKER’S NODE OR ROUTE.
    A worker node or path corresponds to any node that can stick the application symbol in many nodes.

#   IS IT POSSIBLE TO STICK WITH THE APACHE SPARK ON APACHE MESOS?
    Yes, you should adhere to the clusters of resources that have Mesos.

#   WHY IS THERE A NEED FOR TRANSMITTING VARIABLES WHILE USING APACHE SPARK?
    Because it reads, except for variables, the relevant in-memory array on each machine tool.

#   EXPLAIN THE NODE OF THE APACHE SPARK WORKER.
    The node of a worker is any path that can run the application code in a cluster.
    A node or route that can run the Spark program code in a cluster can be called a worker or porter node.

#   DOES SPARK USE HADOOP?
    Spark has its own cluster administration list and only uses Hadoop for collection.

#   WHAT IS THE FUNCTION OF SPARK ENGINE?
    Spark Engine schedules for distribution and monitoring.

#   CAN YOU FLEE APACHE SPARK ON APACHE MESOS?
    Yes, it can flee Apache Spark on the hardware   clusters that Mesos charges.

#    What do you understand by Shuffling in Spark?
    The process of redistribution of data across different partitions which might or might not cause data movement across the JVM processes or the executors on the separate machines is known as shuffling/repartitioning. Partition is nothing but a smaller logical division of data.

#   What is the working of DAG in Spark?
    DAG stands for Direct Acyclic Graph which has a set of finite vertices and edges. The vertices represent RDDs and the edges represent the operations to be performed on RDDs sequentially. The DAG created is submitted to the DAG Scheduler which splits the graphs into stages of tasks based on the transformations applied to the data. The stage view has the details of the RDDs of that stage.

    The working of DAG in spark is defined as per the workflow diagram below:


    The first task is to interpret the code with the help of an interpreter. If you use the Scala code, then the Scala interpreter interprets the code.
    Spark then creates an operator graph when the code is entered in the Spark console.
    When the action is called on Spark RDD, the operator graph is submitted to the DAG Scheduler.
    The operators are divided into stages of task by the DAG Scheduler. The stage consists of detailed step-by-step operation on the input data. The operators are then pipelined together.
    The stages are then passed to the Task Scheduler which launches the task via the cluster manager to work on independently without the dependencies between the stages.
    The worker nodes then execute the task.









