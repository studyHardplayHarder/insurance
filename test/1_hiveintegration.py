# -*- coding: utf-8 -*-
# Program function：
from pyspark.sql import SparkSession
import os

os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/envs/pyspark_env/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    _APP_NAME = "test"
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("hive-integration") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))  # 获取项目根目录
    print(PROJECT_ROOT)#/export/pyfolder1/pyspark-chapter03_3.8/main
    # 查看有哪些表
    spark.sql("show databases").show()
    spark.sql("use default")
    spark.sql("show tables").show()
    # 创建视图（表）
    spark.sql("create temporary view test_tab(id,name) as values(1,'张三'),(2,'李四')")
    # 查询数据
    spark.sql("select * from test_tab ").show()
    spark.stop()