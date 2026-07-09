
Deduplication process 

Dedup will force to shuffle to move all the duplicate data into the same partition for cleaning up. 

Dynamic Partition Coalescing: If you set spark.sql.shuffle.partitions to a high number (like 200) for a large dataset, but a specific dataset in your loop turns out to be tiny, AQE will automatically catch this mid-run. It will merge those 200 partitions down to a smaller number (like 4 or 1) before doing the write, preventing empty file overhead

reduce parition from default 200 to 8. 