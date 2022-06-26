Code Assessment
=============

This is just a demo ETL batch process that takes the input data in CSV format, parses it and performs some aggregations on it and then finally save the output data to a file.
Currently it supports the following file formats with csv being the default one.
  * JSON
  * CSV
  * PARQUET
  * XML (not fully supported yet as it requires external package & setup)


###  Setup
Download the git repo
```
git clone https://github.com/chandanparihar1986/code_assessment_250622.git
```


###  How to Build
```
Pre-requisite
    * Make sure the pyspark package is installed in the current python environment i.e. venv
    * pip install pyspark

cd <local_git_repo_path>
python3 -m main.py
```

###  Run the test
```
cd <local_git_repo_path>
python -m unittest tests/test_assessment.py 
```

###  Out of scope
Parameterization using confile file 
   * All hardcoded values must be passed through the config file i.e. source, target, file path, etc.

CICD - stage or prod deployment

END to End pipeline   (Ingestion, Integration, Processing, Storage & Visualization)

###  Assumption
The ETL pipeline intends to work on large volume of data, possibily caturing memeory stats of millions of hosts around the world at a high frequency i.e. 1 MM/s


###  Performance
To increase the performance of the ETL pipeline, given the raw data contains mainly the numbers, we may consider catching the raw data on executor memory with a predefined partition i.e. say on Host column with low cardinality.
This can be done using spark's caching APIs i.e. df.cache() or df.persist(DISK/MEMORY). 
Also second reason to catch the raw data would be to utilize the executor memory as much as possible since the computation on the data frame is relatively simple
which would most likely not take up a lot of memory.
However, this should only be explored when experiencing slowness in the execution of the ETL pipeline or the pipeline is breaching the SLAs as catching data in memory has its own challenges i.e. out of memory exception, data getting spilled over to disk while shuffling, etc.