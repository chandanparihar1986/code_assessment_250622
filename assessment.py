"""
Author: Chandan Parihar
Date: 25-June-2022
Description: Unit test cases
Date                Name                 Description
25-06-2022           CP                  Demo project
"""

from pyspark.sql import SparkSession
import re
import os
from logger import lLogger

class ETLHostMemoryAggregator:
    def __init__(self):
        self.spark= SparkSession.builder \
        .master("local") \
        .appName("Code-Assessment-250622") \
        .getOrCreate()
        self.table_name = "host_memory_usage"
        self.base_path= os.getcwd()
        self.fields = []

    def normalizeCols(self, df):
        """
            Here I have deliberately used the inferred schema as it will allow more hosts to be added to this dataset in the future,
            also if any existing host gets removed, I'll not need to make any code change as the schema is not enforced into the dataset explicitly.
            Here the default schema fits for the purpose. However, there are changes being made to the column names to make them cleanly readable
            and suitable for the transpose operation.
        """
        new_cols = []
        for col in df.columns:
            col = col.lower()
            new_col = None
            match_value = re.search("(.*)(host\d{1,})(.*)", col)
            if match_value:
                new_col = match_value.group(2)
                new_cols.append(new_col)
            else:
                # Here only one date column is expected in the dataset
                new_col = "date_time" if "date" in col else None
                if not new_col:
                    lLogger.error("Unexpected column : Besides host columns, expected one date column. Please check the CSV file")
                    raise KeyError(
                        "Unexpected column : Besides host columns, expected one date column. Please check the CSV file")

            df = df.withColumnRenamed(col, new_col)

        self.fields = new_cols
        return df


    def getProjectedCols(self):
        projected_cols = ""
        for el in self.fields:
            projected_cols += "'{}'".format(el) + "," + el + ","
        projected_cols = projected_cols[:-1]
        return projected_cols

    def transpose_dataframe(self, df):
        df = self.spark.sql("""
                       select 
                            from_unixtime(date_time,'yyyy-MM-dd HH:mm:ss') date_time, 
                            stack({0},{1}) as (host, free_mem_percentage) 
                        from raw_data
           """.format(len(self.fields), self.getProjectedCols()))

        df.createOrReplaceTempView("{}".format(self.table_name))

    def writeAggPerHost(self, format="csv"):

        df = self.spark.sql("""
                        select 
                                host, 
                                min(free_mem_percentage) min_free_mem_percentage, 
                                max(free_mem_percentage) max_free_mem_percentage,
                                avg(free_mem_percentage) avg_free_mem_percentage 
                            from {} group by host
                        """.format(self.table_name))

        if format.lower()=="json":
            df.write.mode("overwrite") \
                .option("header", "true") \
                .json("{}/output/aggperhost.json".format(self.base_path))
        elif format.lower()=="parquet":
            df.write.mode("overwrite")\
                .option("header","true")\
                .parquet("{}/output/aggperhost.parquet".format(self.base_path))
        elif format.lower()=="csv":
            df.write.mode("overwrite")\
                .option("header","true")\
                .csv("{}/output/aggperhost.csv".format(self.base_path))
        elif format.lower()=="xml":
            df.write \
            .format("com.databricks.spark.xml") \
                .option("rootTag", "items") \
                .option("rowTag", "item") \
                .mode("overwrite")\
                .save("{}/output/aggperhost.xml".format(self.base_path))


    def writeAggAcrossHost(self, format="csv"):
        df = self.spark.sql("""
                                select 
                                        min(free_mem_percentage) min_free_mem_percentage, 
                                        max(free_mem_percentage) max_free_mem_percentage,
                                        avg(free_mem_percentage) avg_free_mem_percentage 
                                    from {} 
                                """.format(self.table_name))
        if format.lower()=="json":
            df.write.mode("overwrite").json("{}/output/aggacrosshost.json".format(self.base_path))
        elif format.lower()=="parquet":
            df.write.mode("overwrite").parquet("{}/output/aggacrosshost.parquet".format(self.base_path))
        elif format.lower()=="csv":
            df.write.mode("overwrite").csv("{}/output/aggacrosshost.csv".format(self.base_path))