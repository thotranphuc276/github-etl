import logging
import os
import time
from datetime import datetime, timedelta

import requests
from dateutil import parser
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GitHubExtractor:
    """
    Class for extracting data from the GitHub API
    """

    def __init__(self, token=None):
        """
        Initialize with GitHub API token

        Args:
            token (str, optional): GitHub API token. If not provided, will try to get from environment.
        """
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            logger.warning("No GitHub token provided. API rate limits will be restrictive.")

        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json"
        }

        if self.token:
            self.headers["Authorization"] = f"token {self.token}"

    def get_repository_info(self, owner, repo):
        """
        Get information about a repository

        Args:
            owner (str): Repository owner
            repo (str): Repository name

        Returns:
            dict: Repository information
        """
        url = f"{self.base_url}/repos/{owner}/{repo}"
        response = self._make_request(url)

        if response:
            return {
                "name": response.get("name"),
                "owner": owner,
                "full_name": response.get("full_name"),
                "description": response.get("description"),
                "url": response.get("html_url"),
                "created_at": parser.parse(response.get("created_at")) if response.get("created_at") else None
            }
        return None

    def get_commits(self, owner, repo, months=6):
        """
        Get commits from a repository over the last X months

        Args:
            owner (str): Repository owner
            repo (str): Repository name
            months (int, optional): Number of months to look back. Defaults to 6.

        Returns:
            list: List of commit data
        """
        logger.info(f"Fetching commits for {owner}/{repo} from the last {months} months")

        since_date = datetime.now() - timedelta(days=30 * months)
        since_str = since_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/repos/{owner}/{repo}/commits"
        params = {
            "since": since_str,
            "per_page": 100
        }

        all_commits = []
        page = 1
        total_pages = None

        with tqdm(desc="Fetching commits", unit="page") as pbar:
            while True:
                params["page"] = page
                response = self._make_request(url, params=params)

                if not response:
                    break

                for commit_data in response:
                    commit = self._process_commit(commit_data)
                    if commit:
                        all_commits.append(commit)

                if len(response) < 100:
                    break

                if not total_pages and "Link" in self._last_response.headers:
                    total_pages = self._get_last_page(self._last_response.headers["Link"])
                    pbar.total = total_pages

                page += 1
                pbar.update(1)

                self._handle_rate_limit()

        logger.info(f"Fetched {len(all_commits)} commits from {owner}/{repo}")
        return all_commits

    @staticmethod
    def _process_commit(commit_data):
        """
        Process a commit response from GitHub API

        Args:
            commit_data (dict): Raw commit data from GitHub API

        Returns:
            dict: Processed commit data
        """
        try:

            github_committer = commit_data.get("committer", {})
            git_committer = commit_data.get("commit", {}).get("committer", {})

            committer = {
                "login": github_committer.get("login") if github_committer else None,
                "name": git_committer.get("name"),
                "email": git_committer.get("email"),
                "avatar_url": github_committer.get("avatar_url") if github_committer else None
            }

            github_author = commit_data.get("author", {})
            git_author = commit_data.get("commit", {}).get("author", {})

            author = {
                "login": github_author.get("login") if github_author else None,
                "name": git_author.get("name"),
                "email": git_author.get("email"),
                "avatar_url": github_author.get("avatar_url") if github_author else None
            }

            return {
                "sha": commit_data.get("sha"),
                "message": commit_data.get("commit", {}).get("message"),
                "committed_at": parser.parse(git_committer.get("date")) if git_committer.get("date") else None,
                "authored_at": parser.parse(git_author.get("date")) if git_author.get("date") else None,
                "committer": committer,
                "author": author
            }
        except Exception as e:
            logger.error(f"Error processing commit: {e}")
            return None

    def _make_request(self, url, params=None):
        """
        Make a request to the GitHub API

        Args:
            url (str): API endpoint
            params (dict, optional): Query parameters

        Returns:
            dict or list: Response data
        """
        try:
            response = requests.get(url, headers=self.headers, params=params)
            self._last_response = response

            if response.status_code == 403 and "rate limit exceeded" in response.text.lower():
                self._handle_rate_limit(response)

                return self._make_request(url, params)

            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logger.error(f"HTTP error: {e}")
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
        except Exception as e:
            logger.error(f"Error making request: {e}")

        return None

    @staticmethod
    def _handle_rate_limit(response=None):
        """
        Handle GitHub API rate limiting

        Args:
            response (requests.Response, optional): Response object
        """
        if response and "X-RateLimit-Reset" in response.headers:
            reset_time = int(response.headers["X-RateLimit-Reset"])
            wait_time = max(reset_time - int(time.time()), 0) + 5

            if wait_time > 0:
                logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds for reset.")
                time.sleep(wait_time)
        else:

            time.sleep(0.5)

    @staticmethod
    def _get_last_page(link_header):
        """
        Extract the last page number from the Link header

        Args:
            link_header (str): Link header from GitHub API

        Returns:
            int: Last page number or None
        """
        if not link_header:
            return None

        links = link_header.split(",")
        for link in links:
            if 'rel="last"' in link:
                url = link.split(";")[0].strip("<>")
                try:
                    return int(url.split("page=")[1].split("&")[0])
                except:
                    pass

        return None
