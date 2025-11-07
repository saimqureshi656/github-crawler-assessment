import requests
import time
from typing import List, Dict, Optional
import sys

class GitHubGraphQLClient:
    """
    Client for GitHub's GraphQL API.
    
    Why GraphQL over REST?
    - Fetch multiple fields in one request (repo + stars + owner)
    - Better rate limits (5000 points/hour vs 5000 requests/hour)
    - Cursor-based pagination (handles millions of results)
    """
    
    def __init__(self, token: str):
        """
        Initialize with GitHub token.
        
        Token is required for:
        - Authentication
        - Higher rate limits
        - Access to API
        """
        if not token:
            raise ValueError("GitHub token is required!")
        
        self.token = token
        self.endpoint = "https://api.github.com/graphql"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        print("✅ GitHub client initialized")
        
    def _execute_query(self, query: str, variables: Dict) -> Dict:
        """
        Execute GraphQL query with retry logic.
        
        Why retry logic?
        - Network can fail temporarily
        - Rate limits need waiting
        - Improves reliability
        
        Retry strategy: Exponential backoff
        - Attempt 1: Immediate
        - Attempt 2: Wait 2 seconds
        - Attempt 3: Wait 4 seconds
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.endpoint,
                    json={"query": query, "variables": variables},
                    headers=self.headers,
                    timeout=30  # Fail if no response in 30s
                )
                
                # GitHub sends rate limit info in headers
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check for GraphQL errors (different from HTTP errors!)
                    if 'errors' in data:
                        print(f"⚠️  GraphQL errors: {data['errors']}")
                        if attempt < max_retries - 1:
                            print(f"Retrying in {2 ** attempt} seconds...")
                            time.sleep(2 ** attempt)
                            continue
                        else:
                            raise Exception(f"GraphQL errors: {data['errors']}")
                    
                    return data
                
                # Handle rate limiting
                if response.status_code == 403 or remaining == 0:
                    wait_time = max(reset_time - time.time() + 10, 60)
                    print(f"⏱️  Rate limit hit. Waiting {wait_time:.0f} seconds...")
                    time.sleep(wait_time)
                    continue
                
                # Handle other HTTP errors
                if response.status_code == 401:
                    raise Exception("Authentication failed. Check your GitHub token!")
                
                response.raise_for_status()
                
            except requests.exceptions.Timeout:
                print(f"⚠️  Request timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            
            except requests.exceptions.RequestException as e:
                print(f"⚠️  Request failed on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
        
        raise Exception("Max retries exceeded")
    
    def fetch_repositories(self, cursor: Optional[str] = None, batch_size: int = 100) -> Dict:
        """
        Fetch repositories using GitHub's search.
        
        Why search query "stars:>1"?
        - Filters out repos with 0 stars
        - Returns ANY repos with stars (not sorted)
        - Gives us diverse sample
        
        Parameters:
        - cursor: Pagination cursor (where to continue from)
        - batch_size: How many repos per request (max 100)
        
        Returns:
        - Dict with repos, pagination info, and rate limit data
        """
        
        # GraphQL query
        # Triple quotes allow multi-line strings
        query = """
        query($cursor: String, $batch_size: Int!) {
          search(query: "stars:>1", type: REPOSITORY, first: $batch_size, after: $cursor) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                id
                databaseId
                name
                nameWithOwner
                owner {
                  login
                }
                stargazerCount
                createdAt
                updatedAt
              }
            }
          }
          rateLimit {
            remaining
            resetAt
            cost
          }
        }
        """
        
        variables = {
            "cursor": cursor,
            "batch_size": batch_size
        }
        
        return self._execute_query(query, variables)
    
    def check_rate_limit(self) -> Dict:
        """
        Check current rate limit status.
        
        Useful for:
        - Monitoring before starting crawl
        - Debugging rate limit issues
        """
        query = """
        query {
          rateLimit {
            limit
            remaining
            resetAt
            cost
          }
        }
        """
        
        response = self._execute_query(query, {})
        rate_limit = response['data']['rateLimit']
        
        print(f"📊 Rate Limit Status:")
        print(f"   Limit: {rate_limit['limit']}")
        print(f"   Remaining: {rate_limit['remaining']}")
        print(f"   Resets at: {rate_limit['resetAt']}")
        
        return rate_limit