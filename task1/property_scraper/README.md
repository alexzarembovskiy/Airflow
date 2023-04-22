# Class task: Property Scraper

### 1) Present diagram of DAG and explain responsibility of each task in the DAG.

- *check_resources* - make a request to web resources to check the availability;
- *extract_Austin_region_URL* - make a request to resource to get Austin region;
- *download_Austin_region_pages* - download region HTML pages;
- *transform_Austin_region_data* - transform HTML pages, extract needed data and store it;
- *extract_Austin_communities_URL* - request to resource to get Austin communities;
- *download_Austin_communities_pages* - download communities HTML pages;
- *transform_Austin_communities_data* - transform HTML pages, extract needed data and store it;
- *extract_Austin_listings_URL* - for every community request to get listings;
- *download_Austin_listings_pages* - download listings HTML pages;
- *transform_Austin_listings_data* - transform HTML pages, extract needed data and store it.

### 2) Describe the approach that you plan to use for data extraction (API calIs, HTML parsing, etc.).

HTML parsing is sound decision, because resource doesn't have an API.

### 3) Propose some mechanism for data caching that will allow you to rerun data transformation without data extraction, when data transformation fails.

Persistent storage on some RDBMS. Data from the most recent runs can be cached in memory - for instance, in Redis.

### 4) Describe all third-party tools (like AWS S3, AWS RDS) that you plan to use in your pipeline.

- AWS S3 - storing raw HTML pages;
- AWS RDS (Postgres) - storing URL and metadata about processed HTML pages, etc;
- AWS MemoryDB (Redis) - storing data from the most recent runs;
- AWS Redshift - central DWH for storing transformed data;
- AWS Glue - data categorization and governance.

### 5) Analyze weak points in your pipeline (for tasks that would have potentially large execution time) and propose improvements that can be done in future.

As web resources may have limitation related to request amount, we should track the number of requests and does not exceed it.
Requesting and downloading HTML pages could take some time, so we won`t have immediate result as in streaming.
To mitigate previously mentioned drawback we can execute our requests in multiple threads.