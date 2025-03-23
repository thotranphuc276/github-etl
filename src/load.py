import logging

from sqlalchemy.orm import Session

from db.models import Base, Repository, Committer, Author, Commit

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GitHubLoader:
    """
    Class for loading GitHub data into the database
    """

    def __init__(self, engine):
        """
        Initialize with database engine

        Args:
            engine (sqlalchemy.engine.Engine): Database engine
        """
        self.engine = engine

        Base.metadata.create_all(engine)

    def load_data(self, repository_data, committers_data, authors_data, commits_data):
        """
        Load GitHub data into the database

        Args:
            repository_data (dict): Repository information
            committers_data (list): List of committer data
            authors_data (list): List of author data
            commits_data (list): List of commit data

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with Session(self.engine) as session:

                repository = self._load_repository(session, repository_data)
                if not repository:
                    return False

                committers_map = self._load_committers(session, committers_data)

                authors_map = self._load_authors(session, authors_data)

                self._load_commits(session, commits_data, repository, committers_map, authors_map)

                session.commit()

                logger.info("Successfully loaded data into the database")
                return True

        except Exception as e:
            logger.error(f"Error loading data into database: {e}")
            return False

    @staticmethod
    def _load_repository(session, repository_data):
        """
        Load repository data into the database

        Args:
            session (sqlalchemy.orm.Session): Database session
            repository_data (dict): Repository information

        Returns:
            Repository: Repository object
        """
        if not repository_data:
            logger.error("No repository data to load")
            return None

        existing_repo = session.query(Repository).filter(
            Repository.full_name == repository_data["full_name"]
        ).first()

        if existing_repo:
            logger.info(f"Repository {repository_data['full_name']} already exists in database")
            return existing_repo

        repository = Repository(**repository_data)
        session.add(repository)
        session.flush()

        logger.info(f"Added repository {repository_data['full_name']} to database")
        return repository

    @staticmethod
    def _load_committers(session, committers_data):
        """
        Load committer data into the database

        Args:
            session (sqlalchemy.orm.Session): Database session
            committers_data (list): List of committer data

        Returns:
            dict: Mapping of committer keys to Committer objects
        """
        committers_map = {}

        for committer_data in committers_data:
            login = committer_data.get("login")
            email = committer_data.get("email")

            if not login and not email:
                continue

            committer_key = login or email

            if login:
                existing_committer = session.query(Committer).filter(
                    Committer.login == login
                ).first()
            else:
                existing_committer = session.query(Committer).filter(
                    Committer.email == email
                ).first()

            if existing_committer:
                committers_map[committer_key] = existing_committer
                continue

            committer = Committer(**committer_data)
            session.add(committer)
            session.flush()

            committers_map[committer_key] = committer

        logger.info(f"Processed {len(committers_map)} committers")
        return committers_map

    @staticmethod
    def _load_authors(session, authors_data):
        """
        Load author data into the database

        Args:
            session (sqlalchemy.orm.Session): Database session
            authors_data (list): List of author data

        Returns:
            dict: Mapping of author keys to Author objects
        """
        authors_map = {}

        for author_data in authors_data:
            login = author_data.get("login")
            email = author_data.get("email")

            if not login and not email:
                continue

            author_key = login or email

            if login:
                existing_author = session.query(Author).filter(
                    Author.login == login
                ).first()
            else:
                existing_author = session.query(Author).filter(
                    Author.email == email
                ).first()

            if existing_author:
                authors_map[author_key] = existing_author
                continue

            author = Author(**author_data)
            session.add(author)
            session.flush()

            authors_map[author_key] = author

        logger.info(f"Processed {len(authors_map)} authors")
        return authors_map

    @staticmethod
    def _load_commits(session, commits_data, repository, committers_map, authors_map):
        """
        Load commit data into the database

        Args:
            session (sqlalchemy.orm.Session): Database session
            commits_data (list): List of commit data
            repository (Repository): Repository object
            committers_map (dict): Mapping of committer keys to Committer objects
            authors_map (dict): Mapping of author keys to Author objects

        Returns:
            int: Number of commits loaded
        """
        count = 0

        for commit_data in commits_data:

            if not commit_data.get("sha"):
                continue

            existing_commit = session.query(Commit).filter(
                Commit.sha == commit_data["sha"]
            ).first()

            if existing_commit:
                continue

            committer_key = commit_data.pop("committer_key", None)
            committer = committers_map.get(committer_key)

            if not committer:
                logger.warning(f"Could not find committer for commit {commit_data['sha']}")
                continue

            author_key = commit_data.pop("author_key", None)
            author = authors_map.get(author_key)

            if not author:
                logger.warning(f"Could not find author for commit {commit_data['sha']}")
                continue

            commit = Commit(
                sha=commit_data["sha"],
                message=commit_data.get("message"),
                committed_at=commit_data["committed_at"],
                authored_at=commit_data.get("authored_at"),
                repository=repository,
                committer=committer,
                author=author
            )

            session.add(commit)
            count += 1

            if count % 100 == 0:
                session.flush()

        logger.info(f"Added {count} new commits to database")
        return count
