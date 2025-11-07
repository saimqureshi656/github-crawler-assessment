# GitHub Crawler

This project crawls public GitHub repositories using the **GitHub GraphQL API** and stores each repository’s star count in a **PostgreSQL** database.  
The entire process runs automatically through **GitHub Actions**, which sets up a Postgres service, runs the crawler, and uploads the results as a CSV artifact.



## How It Works

1. The workflow starts on every push, daily (via cron), or when triggered manually.
2. The crawler (`main.py`) uses GraphQL to fetch repositories in batches of 100.
3. Each batch is stored in the database through upsert operations.
4. After the crawl finishes, results are exported to `github_repos.csv` and uploaded as an artifact.


## Current Configuration

The crawler is configured to collect **100,000 repositories**

**GitHub Actions Summary**
Uses a Postgres service container (no external database needed)
Installs dependencies and sets up schema
Runs the crawler script
Exports data to CSV
Uploads CSV as an artifact for download
Each workflow run produces an artifact such as crawler-results-42 containing the exported CSV file.

Project Structure
graphql
Copy code
src/
 ├── crawler/github_client.py   # GitHub GraphQL API client
 ├── db/connection.py           # PostgreSQL connection and helpers
 ├── main.py                    # Main crawler entry point
scripts/
 └── schema.sql                 # Database schema
.github/
 └── workflows/
     └── crawler.yml            # GitHub Actions pipeline


**Scaling to 500 Million Repositories**
If this system needed to scale from 100 k to 500 million repositories, the following design changes would be required:
Distributed crawling: Use multiple worker instances (Celery, Kafka, or Kubernetes Jobs) to parallelize API calls.
Rate-limit handling: Manage many GitHub tokens with a central coordinator.
Storage layer: Move from a single PostgreSQL instance to sharded Postgres or a data warehouse such as BigQuery or Redshift.
Incremental updates: Store only new or changed data (append-only logs + upserts) to minimize writes.
Partitioning: Split tables by repository ID or crawl date for faster access.
ETL orchestration: Use tools such as Airflow or Dagster for large-scale scheduling, retries, and monitoring.
These changes allow efficient large-scale crawling and updating while keeping each run fault-tolerant and maintainable.

**Tech Stack**
Language: Python 3.11
Database: PostgreSQL
Automation: GitHub Actions
API: GitHub GraphQL API
Libraries: requests, psycopg2-binary

Author
Saim Qureshi
