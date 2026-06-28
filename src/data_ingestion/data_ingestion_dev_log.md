
### Problem 1: 
- Problem context: 26/06/10 15:08:13 WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory. Scaling row group sizes to 95.00% for 8 writers. This happens when read and write 105_000 rows of playback data into MinIO with Delta Lake format. 
- Current solution: Leave it like that because the dataset is small. 
- Possible risks to make the problem worse: If your historical dataset increases from 105,000 rows to a few million rows during a future backfill, those 8 writers will eventually run out of that tiny 1 GB heap space, risking a job failure.
- Possible solution:
    ```
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("IngestionPipeline") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    ```

