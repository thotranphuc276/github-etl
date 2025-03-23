"""
Script to run analysis on previously extracted GitHub commit data
"""

import logging
import os
from dotenv import load_dotenv

from analyze import GitHubAnalyzer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Main entry point for the analysis script
    """

    db_path = os.getenv("DB_PATH", "github_commits.db")

    logger.info(f"Running analysis on database: {db_path}")

    analyzer = GitHubAnalyzer(db_path)
    analyzer.run_all_analyses()

    logger.info("All analyses completed successfully")


if __name__ == "__main__":
    main()
