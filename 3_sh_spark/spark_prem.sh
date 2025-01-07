SPARK_HOME=/export/server/spark
${SPARK_HOME}/bin/spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 4 \
--num-executors 1 \
--class cn.itcast.policy.Main2 \
hdfs:///insurance/insurance_sz24-2.0.jar