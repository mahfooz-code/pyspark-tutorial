#   Tell us something about PySpark SparkFiles?
    
    It is possible to upload our files in Apache Spark.
    We do it by using sc.addFile, where sc is our default SparkContext.
    Also, it helps to get the path on a worker using SparkFiles.get.
    Moreover, it resolves the paths to files which are added through SparkContext.addFile().
    
    It contains some classmethods, such as
    get(filename)
    getrootdirectory()


#   What is PySpark SparkFiles?

    One of the most common PySpark interview questions. PySpark SparkFiles is used to load our files on the Apache Spark application. It is one of the functions under SparkContext and can be called using sc.addFile to load the files on the Apache Spark. SparkFIles can also be used to get the path using SparkFile.get or resolve the paths to files that were added from sc.addFile. The class methods present in the SparkFiles directory are getrootdirectory() and get(filename).


#   What is PySpark SparkConf?

    PySpark SparkConf is mainly used to set the configurations and the parameters when we want to run the application on 
    the local or the cluster.

#   

