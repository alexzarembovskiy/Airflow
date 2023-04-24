# Class task: Property Scraper

### 1) Present diagram of DAG and explain responsibility of each task in the DAG.

1. *create_db* - create DB if does not exists;
2. *ping_webpage* - ping web resources to check the availability;
3. *fetch_Austin_region_data* - request to get Austin region data in HTML format;
4. *process_Austin_region_data* - transorm HTML pages; 
5. *write_Austin_region_data* - write data to persistent storage;
6. *fetch_Austin_communities_data* - request to get Austin communities data in HTML format;
7. *process_Austin_communities_data* - transorm HTML pages;
8. *write_Austin_communities_data* - write data to persistent storage;
9. *fetch_Austin_listings_data* - request to get Austin listings data in HTML format;
10. *process_Austin_listings_data* - transorm HTML pages; 
11. *write_Austin_listings_data* - write data to persistent storage.

### 2) Describe the approach that you plan to use for data extraction (API calIs, HTML parsing, etc.).

HTML parsing is sound decision, because resource doesn't have an API.

### 3) Propose some mechanism for data caching that will allow you to rerun data transformation without data extraction, when data transformation fails.

Persistent storage on some RDBMS. Data from the most recent runs can be cached in memory - for instance, in aid of Redis.

### 4) Describe all third-party tools (like AWS S3, AWS RDS) that you plan to use in your pipeline.

- AWS S3 - storing raw HTML pages;
- AWS RDS (Postgres) - storing URLs, metadata about processed HTML pages and historical data;
- AWS MemoryDB (Redis) - storing data from the most recent runs.

### 5) Analyze weak points in your pipeline (for tasks that would have potentially large execution time) and propose improvements that can be done in future.

- If the amount of data grows, we may need to use dedicated warehouse for historical data. For instance, store staging data in Postgres, but final data will be stored in AWS Redshift. 
- As web resources may have limitation related to request amount threshold, we should track the number of requests and does not exceed it.
- We won`t have immediate result as in streaming, as requesting and downloading HTML pages could take some time.
- To mitigate previously mentioned drawback we can execute our requests in multiple threads.