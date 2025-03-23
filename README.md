# GitHub Commits ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for GitHub commits data using the GitHub API.

## Overview

The pipeline extracts commit data from a specified GitHub repository for the last X months, transforms it, and loads it into a SQLite database. It then performs various analyses on the data.

## Features

- Extracts commits from any GitHub repository for the last X months
- Stores data in a SQLite database (refreshed on each run)
- Provides SQL queries to:
  - Find the top 5 authors by commit count
  - Find the top 5 committers by commit count (for comparison)
  - Determine the author with the longest commit streak
  - Generate a heatmap of commits by day of week and time of day

## Requirements

- Python 3.8+
- Dependencies in `requirements.txt`

## Installation

```bash
pip install -r requirements.txt
```

## Usage

1. Copy the example environment file and edit it with your settings:
```bash
cp .env.example .env
# Edit .env with your preferred text editor
```

2. Configure the following variables in the `.env` file:
```
GITHUB_TOKEN=your_token_here
GITHUB_REPO=owner/repo_name
MONTHS=6
DB_PATH=github_commits.db
RUN_ANALYSIS=true
```

3. Run the ETL pipeline:
```bash
./run.sh
```

Alternatively, you can run the Python script directly:
```bash
python src/main.py
```

**Note**: Each run refreshes the database, deleting any existing data.

4. To run the analysis separately:
```bash
python src/analyze.py
```

## Configuration

The following environment variables can be set in the `.env` file:

| Variable      | Description                              | Default         |
|---------------|------------------------------------------|-----------------|
| GITHUB_TOKEN  | Your GitHub API token                    | None            |
| GITHUB_REPO   | Repository to analyze (owner/repo_name)  | Required        |
| MONTHS        | Number of months to look back            | 6               |
| DB_PATH       | Path to SQLite database file             | github_commits.db |
| RUN_ANALYSIS  | Whether to run analysis after ETL        | true            |

## Architecture

The ETL pipeline is composed of the following components:

### Extract
- Uses the GitHub API to fetch repository information and commits
- Handles pagination and rate limiting
- Processes the raw API responses into a more usable format

### Transform
- Cleans and transforms the extracted data
- Deduplicates committers and authors
- Structures the data for database loading

### Load
- Creates the necessary database tables
- Loads the transformed data into the SQLite database
- Handles duplicates and relationships between entities

### Analysis
- Queries the database for insights
- Generates visualizations
- Exports results to CSV files

## Author vs Committer

In Git, there's a distinction between authors and committers:

- **Author**: The person who originally wrote the code
- **Committer**: The person who committed the code to the repository

For most commits, the author and committer are the same person. However, they can differ in scenarios like:
- Code review workflows where someone approves and merges code written by another person
- Merges done through GitHub's web interface (where the committer might be "web-flow")
- Rebased or amended commits
- Patches applied by someone other than the original author

This ETL pipeline captures both author and committer information for analysis.

## SQL Queries

The pipeline includes the following key SQL queries:

### Top 5 Authors
```sql
SELECT 
    COALESCE(a.login, a.name, a.email, 'Unknown') as author,
    COUNT(cm.id) as commit_count
FROM
    commits cm
JOIN
    authors a ON cm.author_id = a.id
GROUP BY
    a.id
ORDER BY
    commit_count DESC
LIMIT 5
```

### Longest Author Streak
```sql
WITH daily_commits AS (
    -- Get one record per author per day
    SELECT
        a.id as author_id,
        COALESCE(a.login, a.name, a.email, 'Unknown') as author,
        DATE(cm.authored_at) as commit_date
    FROM
        commits cm
    JOIN
        authors a ON cm.author_id = a.id
    GROUP BY
        a.id, DATE(cm.authored_at)
),

-- Number the days to identify gaps
numbered_days AS (
    SELECT
        author_id,
        author,
        commit_date,
        ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY commit_date) as row_num,
        julianday(commit_date) - ROW_NUMBER() OVER (PARTITION BY author_id ORDER BY commit_date) as group_id
    FROM
        daily_commits
),

-- Count streak lengths
streaks AS (
    SELECT
        author_id,
        author,
        MIN(commit_date) as streak_start,
        MAX(commit_date) as streak_end,
        COUNT(*) as streak_length
    FROM
        numbered_days
    GROUP BY
        author_id, group_id
)

-- Get the longest streak
SELECT
    author,
    streak_start,
    streak_end,
    streak_length
FROM
    streaks
ORDER BY
    streak_length DESC
LIMIT 1
```

### Commit Heatmap
```sql
SELECT
    -- 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    CAST(strftime('%w', authored_at) AS INTEGER) as day_of_week,
    -- Hour of day (0-23)
    CAST(strftime('%H', authored_at) AS INTEGER) as hour,
    COUNT(*) as commit_count
FROM
    commits
GROUP BY
    day_of_week, hour
ORDER BY
    day_of_week, hour
```

## Output

The pipeline generates:
- CSV files with analysis results
- Visualizations of the results
- SQLite database with the extracted data

All output files are stored in the `output` directory.

## Project Structure

- `src/`
  - `main.py`: Entry point for the ETL pipeline
  - `extract.py`: GitHub API data extraction
  - `transform.py`: Data transformation logic
  - `load.py`: Database loading functionality
  - `analyze.py`: SQL analysis and reporting
  - `utils.py`: Utility functions
  - `db/`: Database-related code
    - `models.py`: Database models
    - `db_utils.py`: Database utility functions