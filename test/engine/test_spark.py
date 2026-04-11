from pipeforge.engine.spark import SparkWrapper
import unittest 

class TestSparkEngine(unittest.TestCase): 

    def test_s3_bucket_creation(self):

        conf ={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
        }
        spark = SparkWrapper(spark_conf=conf).spark
        print("Spark version:", spark.version)
        # After obtaining spark
        confs = spark.sparkContext.getConf().getAll()
        driver_memory = "0"
        executor_memory = "0"
        for k, v in confs:
            if k.startswith("spark.driver.memory"):
                driver_memory = v 
            elif k.startswith("spark.executor.memory"):
                executor_memory = v
      
        self.assertEqual("2g", driver_memory)
        self.assertEqual("4g", executor_memory)

