# ASSIGNMENT 2

1. Create different parquet files for each date between 2018-01-01 and 2020-23-10. Name of the parquet file should follow the nomenclature '`TSK_YYYYMMDD.parquet`' 

2. Load all parquets under different folders in Data Lake basis their Year-Month. Ex : File `TSK_20190201` should be loaded to folder '`201902`' 

3. Create a read function in Databricks that takes 2 dates and reads from ADLS only those parquet files that are lying between that date range. Create a single dataframe out of all the read parquets

# SOLUTION

To address this assignment, you will need to perform the following steps in Databricks:

1. **Create different parquet files for each date between 2018-01-01 and 2020-10-23.**
2. **Load all parquet files under different folders in Data Lake based on their Year-Month.**
3. **Create a read function in Databricks to read parquet files between two given dates and create a single DataFrame.**

Here's a step-by-step guide:

### Step 1: Create Different Parquet Files for Each Date

First, generate the dates and create the parquet files.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pandas as pd

spark = SparkSession.builder.appName("ParquetFilesCreation").getOrCreate()

start_date = "2018-01-01"
end_date = "2020-10-23"
date_range = pd.date_range(start=start_date, end=end_date)

for date in date_range:
    date_str = date.strftime("%Y%m%d")
    df = spark.createDataFrame([(date_str,)], ["Date"])
    df = df.withColumn("dummy_data", lit("some_value"))
    df.write.parquet(f"/mnt/data/{date.year}{date.month:02d}/TSK_{date_str}.parquet")
```

### Step 2: Load All Parquet Files Under Different Folders in Data Lake

The above code already saves the parquet files in folders based on their Year-Month format.

### Step 3: Create a Read Function to Read Parquet Files Between Two Dates

Now, create a function that reads parquet files between two dates and creates a single DataFrame.

```python
from datetime import datetime

def read_parquet_files_between_dates(start_date, end_date):
    # Convert string dates to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate the date range
    date_range = pd.date_range(start=start_date, end=end_date)

    # Collect all parquet file paths
    parquet_files = []
    for date in date_range:
        folder_path = f"/mnt/data/{date.year}{date.month:02d}/"
        file_path = f"{folder_path}TSK_{date.strftime('%Y%m%d')}.parquet"
        parquet_files.append(file_path)

    # Read all parquet files into a single DataFrame
    df = spark.read.parquet(*parquet_files)
    
    return df

# Example usage
start_date = "2019-01-01"
end_date = "2019-01-10"
df = read_parquet_files_between_dates(start_date, end_date)
df.show()
```

