# TESTED : Working
spark-submit \
    --driver-class-path /app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar \
    --jars /app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar spark-oracle-ds.py

# Config jars in application code -- TESTED : Working
spark-submit \
    --driver-class-path /app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar \
    spark-oracle-ds.py

# without configuring jar in application code -- TESTED : Working
spark-submit \
    --driver-class-path /app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar \
    spark-oracle-ds.py


#WORKING
spark-submit \
    --master yarn    \
    --driver-class-path /app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar \
    spark-oracle-ds.py

#Configuration for Database Jars
export SPARK_CLASSPATH=/app/hadoop_users/MNAAS/MNAAS_Jar/Loader_Jars/ojdbc7.jar




