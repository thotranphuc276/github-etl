import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GitHubTransformer:
    """
    Class for transforming GitHub data before loading into the database
    """

    def __init__(self):
        """
        Initialize the transformer
        """
        pass

    def transform_data(self, repo_info, commits_data):
        """
        Transform repository and commits data

        Args:
            repo_info (dict): Repository information
            commits_data (list): List of commit data

        Returns:
            tuple: (repository dict, list of committers dicts, list of authors dicts, list of commits dicts)
        """
        logger.info("Transforming GitHub data")

        repository = self._transform_repository(repo_info)

        committers_dict = {}
        authors_dict = {}
        transformed_commits = []

        for commit in commits_data:
            if not commit:
                continue

            committer = self._transform_committer(commit.get("committer", {}))
            committer_key = committer.get("login") or committer.get("email")

            if committer_key:
                committers_dict[committer_key] = committer

            author = self._transform_author(commit.get("author", {}))
            author_key = author.get("login") or author.get("email")

            if author_key:
                authors_dict[author_key] = author

            transformed_commit = self._transform_commit(commit, committer_key, author_key)
            if transformed_commit:
                transformed_commits.append(transformed_commit)

        committers = list(committers_dict.values())
        authors = list(authors_dict.values())

        logger.info(
            f"Transformed 1 repository, {len(committers)} committers, {len(authors)} authors, and "
            f"{len(transformed_commits)} commits")

        return repository, committers, authors, transformed_commits

    @staticmethod
    def _transform_repository(repo_info):
        """
        Transform repository data

        Args:
            repo_info (dict): Repository information

        Returns:
            dict: Transformed repository data
        """
        if not repo_info:
            return None

        return {
            "name": repo_info.get("name"),
            "owner": repo_info.get("owner"),
            "full_name": repo_info.get("full_name"),
            "description": repo_info.get("description"),
            "url": repo_info.get("url"),
            "created_at": repo_info.get("created_at")
        }

    @staticmethod
    def _transform_committer(committer_data):
        """
        Transform committer data

        Args:
            committer_data (dict): Committer information

        Returns:
            dict: Transformed committer data
        """
        return {
            "login": committer_data.get("login"),
            "name": committer_data.get("name"),
            "email": committer_data.get("email"),
            "avatar_url": committer_data.get("avatar_url")
        }

    @staticmethod
    def _transform_author(author_data):
        """
        Transform author data

        Args:
            author_data (dict): Author information

        Returns:
            dict: Transformed author data
        """
        return {
            "login": author_data.get("login"),
            "name": author_data.get("name"),
            "email": author_data.get("email"),
            "avatar_url": author_data.get("avatar_url")
        }

    @staticmethod
    def _transform_commit(commit_data, committer_key, author_key):
        """
        Transform commit data

        Args:
            commit_data (dict): Commit information
            committer_key (str): Key to identify the committer
            author_key (str): Key to identify the author

        Returns:
            dict: Transformed commit data
        """
        if not commit_data or not commit_data.get("sha") or not commit_data.get("committed_at"):
            return None

        return {
            "sha": commit_data.get("sha"),
            "message": commit_data.get("message"),
            "committed_at": commit_data.get("committed_at"),
            "authored_at": commit_data.get("authored_at"),
            "committer_key": committer_key,
            "author_key": author_key
        }
