import os
import sys
import time
from crawler.github_client import GitHubGraphQLClient
from db.connection import DatabaseManager

def main():
    """
    Main crawler orchestration.
    
    Flow:
    1. Get GitHub token from environment
    2. Connect to database
    3. Setup schema
    4. Crawl repositories
    5. Export results
    """
    
    print("=" * 60)
    print("🚀 GitHub Repository Crawler")
    print("=" * 60)
    
    # Step 1: Get credentials from environment variables
    # Environment variables are how we pass secrets securely
    github_token = os.environ.get('GITHUB_TOKEN')
    if not github_token:
        print("❌ ERROR: GITHUB_TOKEN environment variable not found")
        print("   Set it with: export GITHUB_TOKEN=your_token_here")
        sys.exit(1)
    
    # Database connection string
    # Format: postgresql://user:password@host:port/database
    db_connection = os.environ.get(
        'DATABASE_URL',
        'postgresql://postgres:postgres@localhost:5432/github_crawler'
    )
    
    print(f"📍 Database: {db_connection.split('@')[1] if '@' in db_connection else 'localhost'}")
    print()
    
    # Step 2: Initialize clients
    try:
        client = GitHubGraphQLClient(github_token)
        db = DatabaseManager(db_connection)
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        sys.exit(1)
    
    try:
        # Step 3: Connect to database
        db.connect()
        
        # Step 4: Setup schema (creates tables if they don't exist)
        db.setup_schema('src/db/schema.sql')
        
        # Check initial state
        initial_count = db.get_total_repos()
        print(f"📊 Repositories in database: {initial_count}")
        print()
        
        # Step 5: Check rate limit before starting
        print("🔍 Checking GitHub API rate limit...")
        client.check_rate_limit()
        print()
        
        # Step 6: Crawl repositories
        target = 100000  # Assignment requirement
        total_repos = 0
        cursor = None
        batch_size = 100  # Maximum allowed by GitHub GraphQL API
        
        print(f"📥 Starting crawl for {target:,} repositories...")
        print(f"   Batch size: {batch_size} repos/request")
        print()
        
        start_time = time.time()
        
        # Main crawling loop
        while total_repos < target:
            try:
                # Fetch batch of repositories
                response = client.fetch_repositories(cursor, batch_size)
                
                # Check if we got valid data
                if 'data' not in response or not response['data']['search']['nodes']:
                    print("⚠️  No more repositories found")
                    break
                
                repos = response['data']['search']['nodes']
                
                # Filter out any null entries (sometimes happens with deleted repos)
                repos = [r for r in repos if r and r.get('databaseId')]
                
                if not repos:
                    print("⚠️  Empty batch, continuing...")
                    continue
                
                # Store in database
                db.upsert_repositories(repos)
                db.insert_star_counts(repos)
                
                total_repos += len(repos)
                
                # Progress update
                elapsed = time.time() - start_time
                rate = total_repos / elapsed if elapsed > 0 else 0
                remaining = target - total_repos
                eta = remaining / rate if rate > 0 else 0
                
                print(f"📈 Progress: {total_repos:,}/{target:,} repos ({total_repos/target*100:.1f}%)")
                print(f"   Rate: {rate:.1f} repos/sec | ETA: {eta/60:.1f} minutes")
                
                # Show rate limit info
                if 'rateLimit' in response['data']:
                    rate_limit = response['data']['rateLimit']
                    print(f"   API Rate Limit: {rate_limit['remaining']} remaining")
                
                print()
                
                # Check if more pages exist
                page_info = response['data']['search']['pageInfo']
                if not page_info['hasNextPage']:
                    print("✅ Reached end of available repositories")
                    break
                
                cursor = page_info['endCursor']
                
                # Small delay to be nice to GitHub's servers
                time.sleep(0.1)
                
            except KeyboardInterrupt:
                print("\n⚠️  Crawl interrupted by user")
                break
                
            except Exception as e:
                print(f"❌ Error during crawl: {e}")
                print("   Continuing to next batch...")
                continue
        
        # Step 7: Summary
        elapsed_time = time.time() - start_time
        final_count = db.get_total_repos()
        
        print("=" * 60)
        print("✅ Crawl Complete!")
        print("=" * 60)
        print(f"📊 Total repositories: {final_count:,}")
        print(f"📊 New repositories: {final_count - initial_count:,}")
        print(f"⏱️  Time taken: {elapsed_time/60:.2f} minutes")
        print(f"⚡ Average rate: {total_repos/elapsed_time:.1f} repos/second")
        print()
        
        # Step 8: Export results
        print("💾 Exporting results to CSV...")
        db.export_to_csv('github_repos.csv')
        print("✅ Export complete: github_repos.csv")
        print()
        
        print("🎉 All done!")
        
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Always close database connection
        db.close()

if __name__ == "__main__":
    main()