"""
Author: Chandan Parihar
Date: 25-June-2022
Description: Unit test cases
Date                Name                 Description
25-06-2022           CP                  Demo project
"""

from pathlib import Path
import unittest
from assessment import ETLHostMemoryAggregator
from logger import lLogger

class ETLHostMemoryAggregatorTest(unittest.TestCase):

    def setUp(self) -> None:
        lLogger.info("Setting up ETL processor to work with")
        self.etlprocessor = ETLHostMemoryAggregator()
        self.path = Path(self.etlprocessor.base_path)
        self.etlprocessor.base_path = self.path.parent.absolute()
        df_raw = self.etlprocessor.spark.read.csv("{}/input/assessment.csv".format(self.etlprocessor.base_path ), header=True)
        df_raw = self.etlprocessor.normalizeCols(df_raw)
        df_raw.createOrReplaceTempView("raw_data")
        self.etlprocessor.transpose_dataframe(df_raw)
        lLogger.info("Setup completed")

    """ Test the min value for this host with csv format
        host#HOST1#% Free Memory avg 1 h
        77.85
        77.86
        77.87
        77.88
        77.89
    """
    def testMinAggPerHost(self):
        lLogger.info("Running test: MinAggregationPerHost")
        self.etlprocessor.writeAggPerHost(format="csv")
        df = self.etlprocessor.spark.read.csv("{}/output/aggperhost.csv".format(self.etlprocessor.base_path), header=True)
        min_percentage = df.where("host=='host1'").selectExpr("min_free_mem_percentage").collect()
        self.assertEqual(min_percentage[0][0], str(77.85),"Unexpected value")
        lLogger.info("Completed test: MinAggregationPerHost")

    """ Test max value for this host with json format
        host#HOST2#% Free Memory avg 1 h
        77.71
        77.7
        77.69
        77.72
        77.73
    """
    def testMaxAggPerHost(self):
        lLogger.info("Running test: MinAggregationAccossHost")
        self.etlprocessor.writeAggPerHost(format="json")
        df = self.etlprocessor.spark.read.option("header","true").json("{}/output/aggperhost.json".format(self.etlprocessor.base_path))
        max_percentage = df.where("host=='host2'").selectExpr("max_free_mem_percentage").collect()
        self.assertEqual(max_percentage[0][0], str(77.73),"Unexpected value")
        lLogger.info("Completed test: MinAggregationAccossHost")