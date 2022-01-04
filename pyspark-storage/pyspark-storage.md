#   What is PySpark StorageLevel?
    
    PySpark StorageLevel is used to control how the RDD is stored, take decisions on where the RDD will be stored 
    (on memory or over the disk or both), and whether we need to replicate the RDD partitions or to serialize the RDD.
    The code for StorageLevel is as follows: 
    
    class pyspark.StorageLevel( useDisk, useMemory, useOfHeap, deserialized, replication = 1)

#   EXPLAIN THE CONCEPT OF PERSISTENCE
    RDD persistence is an ideal technique that saves the results of the RDD assessment.

#   DOES SPARK ALSO CONTAIN THE STORAGE LAYER?
    No, it doesn't have a disc layer, but it lets you use many data sources.

#   WHICH IS THE APACHE SPARK DEFAULT LEVEL?
    The cache() method is used for the default storage level, which is StorageLevel.

#   

