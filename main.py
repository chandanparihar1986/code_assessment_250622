"""
Author: Chandan Parihar
Date: 25-June-2022
Description: Unit test cases
Date                Name                 Description
25-06-2022           CP                  Demo project
"""

from assessment import ETLHostMemoryAggregator
from logger import lLogger

# Demo entry point for this pipeline
if __name__ == '__main__':
    lLogger.info("Pipeline started")
    etlprocessor = ETLHostMemoryAggregator()

    df_raw = etlprocessor.spark.read.csv("input/assessment.csv",header=True)

    df_raw = etlprocessor.normalizeCols(df_raw)
    df_raw.createOrReplaceTempView("raw_data")

    etlprocessor.transpose_dataframe(df_raw)

    etlprocessor.writeAggPerHost(format="json")
    #df = etlprocessor.spark.read.option("header","true").json("{}/output/aggperhost.json".format(etlprocessor.base_path))
    #df.show(5)
    #etlprocessor.writeAggPerHost(format="parquet")
    #etlprocessor.writeAggPerHost()

    #etlprocessor.writeAggAcrossHost(format="json")
    #etlprocessor.writeAggAcrossHost(format="parquet")
    etlprocessor.writeAggAcrossHost()
    df = etlprocessor.spark.read.csv("{}/output/aggacrosshost.csv".format(etlprocessor.base_path), header=True)
    df.show(5)
    lLogger.info("Pipeline completed")
