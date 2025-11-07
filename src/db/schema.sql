-- Main table: Stores repository information
CREATE TABLE IF NOT EXISTS repositories (
    id BIGINT PRIMARY KEY,              -- GitHub's unique repo ID
    name VARCHAR(255) NOT NULL,         -- Repo name (e.g., "react")
    owner VARCHAR(255) NOT NULL,        -- Owner username (e.g., "facebook")
    full_name VARCHAR(512) NOT NULL,    -- Full name (e.g., "facebook/react")
    created_at TIMESTAMP,               -- When repo was created
    updated_at TIMESTAMP,               -- Last repo update
    last_crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we crawled it
    UNIQUE(full_name)                   -- Prevent duplicates
);

-- Separate table for star counts (historical tracking)
CREATE TABLE IF NOT EXISTS repository_stars (
    id SERIAL PRIMARY KEY,              -- Auto-incrementing ID
    repository_id BIGINT NOT NULL REFERENCES repositories(id),  -- Links to repo
    star_count INTEGER NOT NULL,        -- Number of stars
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When we recorded this
    UNIQUE(repository_id, recorded_at)  -- One record per repo per timestamp
);

-- Speed up queries
CREATE INDEX IF NOT EXISTS idx_repo_stars_repo_id ON repository_stars(repository_id);
CREATE INDEX IF NOT EXISTS idx_repo_stars_recorded_at ON repository_stars(recorded_at);
CREATE INDEX IF NOT EXISTS idx_repo_last_crawled ON repositories(last_crawled_at);
