import logging
import os
import sys

from dotenv import load_dotenv

from analyze import GitHubAnalyzer
from db.db_utils import get_db_engine
from extract import GitHubExtractor
from load import GitHubLoader
from transform import GitHubTransformer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_config():
    """
    Get configuration from environment variables

    Returns:
        dict: Configuration parameters
    """

    repo = os.getenv("GITHUB_REPO")
    if not repo:
        logger.error("GITHUB_REPO environment variable is required")
        sys.exit(1)

    if '/' not in repo:
        logger.error("Repository must be in the format 'owner/repo_name'")
        sys.exit(1)

    token = os.getenv("GITHUB_TOKEN")
    months = int(os.getenv("MONTHS", 6))
    db_path = os.getenv("DB_PATH", "github_commits.db")
    run_analysis = os.getenv("RUN_ANALYSIS", "true").lower() in ["true", "yes", "1"]

    config = {
        "repo": repo,
        "token": token,
        "months": months,
        "db_path": db_path,
        "run_analysis": run_analysis
    }

    log_config = config.copy()
    if log_config.get("token"):
        log_config["token"] = "****"
    logger.info(f"Configuration: {log_config}")

    return config


def refresh_database(db_path):
    """
    Delete the existing database file if it exists

    Args:
        db_path (str): Path to SQLite database file
    """
    if os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.info(f"Deleted existing database file: {db_path}")
        except Exception as e:
            logger.error(f"Failed to delete database file: {e}")
            sys.exit(1)


def main():
    """
    Main entry point for the ETL pipeline
    """

    config = get_config()

    # Always refresh the database (delete the existing file)
    refresh_database(config["db_path"])

    owner, repo = config["repo"].split('/')

    if not config["token"]:
        logger.warning("No GitHub API token provided. Rate limits will be restrictive.")

    success = run_etl_pipeline(owner, repo, config["months"], config["token"], config["db_path"])

    if success and config["run_analysis"]:
        run_analysis(config["db_path"])


def run_etl_pipeline(owner, repo, months, token, db_path):
    """
    Run the ETL pipeline

    Args:
        owner (str): Repository owner
        repo (str): Repository name
        months (int): Number of months to look back
        token (str): GitHub API token
        db_path (str): Path to SQLite database file

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Starting ETL pipeline for {owner}/{repo}")

    extractor = GitHubExtractor(token)
    repo_info = extractor.get_repository_info(owner, repo)

    if not repo_info:
        logger.error(f"Failed to get repository info for {owner}/{repo}")
        return False

    commits = extractor.get_commits(owner, repo, months)

    if not commits:
        logger.error(f"Failed to get commits for {owner}/{repo}")
        return False

    transformer = GitHubTransformer()
    repository, committers, authors, transformed_commits = transformer.transform_data(repo_info, commits)

    engine = get_db_engine(db_path)
    loader = GitHubLoader(engine)
    success = loader.load_data(repository, committers, authors, transformed_commits)

    if success:
        logger.info(f"ETL pipeline completed successfully for {owner}/{repo}")
    else:
        logger.error(f"ETL pipeline failed for {owner}/{repo}")

    return success


def run_analysis(db_path):
    """
    Run analysis on the loaded data

    Args:
        db_path (str): Path to SQLite database file
    """
    logger.info("Starting analysis")

    analyzer = GitHubAnalyzer(db_path)
    analyzer.run_all_analyses()

    logger.info("Analysis completed")


if __name__ == "__main__":
    main()
