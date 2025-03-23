from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import os
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_engine(db_path="github_commits.db"):
    """
    Create and return a SQLAlchemy engine connected to the SQLite database.
    
    Args:
        db_path (str): Path to the SQLite database file
    
    Returns:
        sqlalchemy.engine.Engine: Database engine
    """

    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    

    db_url = f"sqlite:///{db_path}"
    engine = create_engine(db_url)
    logger.info(f"Database engine created at {db_path}")
    
    return engine

def get_session(engine):
    """
    Create and return a SQLAlchemy session.
    
    Args:
        engine (sqlalchemy.engine.Engine): Database engine
    
    Returns:
        sqlalchemy.orm.Session: Database session
    """
    Session = sessionmaker(bind=engine)
    return Session()

def execute_query(engine, query, params=None):
    """
    Execute a SQL query and return the results.
    
    Args:
        engine (sqlalchemy.engine.Engine): Database engine
        query (str): SQL query to execute
        params (dict, optional): Parameters for the query
    
    Returns:
        list: Query results as a list of dictionaries
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})

        return [dict(row._mapping) for row in result]