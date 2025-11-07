import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict
import os

class DatabaseManager:
    """
    Handles all database operations.
    
    Why a separate class?
    - Separation of concerns: DB logic isolated from business logic
    - Reusability: Can use this class in multiple places
    - Testability: Easy to mock for testing
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize with connection string.
        
        Example: postgresql://user:password@localhost:5432/dbname
        """
        self.connection_string = connection_string
        self.conn = None
        
    def connect(self):
        """
        Establish connection to PostgreSQL.
        
        Why autocommit=False?
        - We want manual control over transactions
        - If something fails, we can rollback
        """
        self.conn = psycopg2.connect(self.connection_string)
        self.conn.autocommit = False
        print("✅ Database connection established")
        
    def setup_schema(self, schema_file: str):
        """
        Create tables from SQL file.
        
        Why from file?
        - SQL is cleaner than Python strings
        - Easy to version control
        - Can test SQL separately
        """
        with open(schema_file, 'r') as f:
            schema_sql = f.read()
        
        with self.conn.cursor() as cur:
            cur.execute(schema_sql)
        self.conn.commit()
        print("✅ Database schema created")
    
    def upsert_repositories(self, repos: List[Dict]):
        """
        Insert repositories or update if they exist.
        
        Why UPSERT (INSERT ... ON CONFLICT)?
        - If repo exists (same ID), update the timestamp
        - If repo is new, insert it
        - One query handles both cases (efficient!)
        
        Why execute_batch?
        - Sends multiple rows in one round-trip to DB
        - 100x faster than individual INSERTs
        """
        query = """
        INSERT INTO repositories (id, name, owner, full_name, created_at, updated_at, last_crawled_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at,
            last_crawled_at = CURRENT_TIMESTAMP
        """
        
        # Transform data into tuples for insertion
        data = [
            (
                repo['databaseId'],          # GitHub's numeric ID
                repo['name'],                 # Repo name
                repo['owner']['login'],       # Owner username
                repo['nameWithOwner'],        # Full name (owner/repo)
                repo['createdAt'],            # Creation timestamp
                repo['updatedAt']             # Last update timestamp
            )
            for repo in repos
        ]
        
        with self.conn.cursor() as cur:
            execute_batch(cur, query, data, page_size=1000)
        self.conn.commit()
        print(f"✅ Upserted {len(repos)} repositories")
    
    def insert_star_counts(self, star_data: List[Dict]):
        """
        Insert star counts (time-series data).
        
        Why separate from repositories?
        - Stars change frequently
        - We want historical tracking
        - Append-only (immutable) - never UPDATE
        
        Why ON CONFLICT DO NOTHING?
        - If we already recorded stars today, skip it
        - Prevents duplicate entries
        """
        query = """
        INSERT INTO repository_stars (repository_id, star_count, recorded_at)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (repository_id, recorded_at) DO NOTHING
        """
        
        data = [
            (repo['databaseId'], repo['stargazerCount'])
            for repo in star_data
        ]
        
        with self.conn.cursor() as cur:
            execute_batch(cur, query, data, page_size=1000)
        self.conn.commit()
        print(f"✅ Inserted {len(star_data)} star counts")
    
    def get_total_repos(self) -> int:
        """Get count of repositories in database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM repositories")
            return cur.fetchone()[0]
    
    def export_to_csv(self, output_file: str):
        """
        Export data to CSV file.
        
        Why COPY?
        - PostgreSQL's fastest export method
        - Streams directly to file
        - No Python overhead
        """
        query = """
        SELECT 
            r.id,
            r.full_name,
            r.owner,
            r.name,
            rs.star_count,
            rs.recorded_at
        FROM repositories r
        JOIN repository_stars rs ON r.id = rs.repository_id
        ORDER BY rs.star_count DESC
        """
        
        with self.conn.cursor() as cur:
            with open(output_file, 'w') as f:
                # COPY exports directly to file
                cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", f)
        print(f"✅ Exported data to {output_file}")
    
    def close(self):
        """Clean up connection."""
        if self.conn:
            self.conn.close()
            print("✅ Database connection closed")
