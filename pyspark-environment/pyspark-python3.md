#   Running Spark Python Applications

    To determine which dependencies are required on the cluster, you must understand that Spark code applications run in Spark executor processes distributed throughout the cluster.
    
    If the Python transformations you define use any third-party libraries, such as NumPy or nltk, Spark executors require access to those libraries when they run on remote executors.

#   Python Requirements
    Spark 2 requires Python 2.7 or higher, and supports Python 3.
    You might need to install a new version of Python on all hosts in the cluster, because some Linux distributions come with Python 2.6 by default.

#   Setting Python version
     If the right level of Python is not picked up by default, set the environment variables to point to the correct Python executable before running the pyspark command.
     
     export PYSPARK_PYTHON=<python installation location>
     export PYSPARK_DRIVER_PYTHON = <python version>

#   Setting the Python Path

    When Anaconda is installed, it automatically writes its values for spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON and spark.yarn.appMasterEnv.PYSPARK_PYTHON into spark-defaults.conf.
    If Anaconda is installed, values for these parameters set in Cloudera Manager are not used.

#   Complex Dependencies


#   Installing and Maintaining Python Environments

    
    

     

