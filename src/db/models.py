from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table, Boolean, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Repository(Base):
    """
    Represents a GitHub repository
    """
    __tablename__ = 'repositories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    owner = Column(String(255), nullable=False)
    full_name = Column(String(255), unique=True, nullable=False)
    description = Column(Text, nullable=True)
    url = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=True)
    

    commits = relationship("Commit", back_populates="repository")
    
    def __repr__(self):
        return f"<Repository(name='{self.name}', owner='{self.owner}')>"

class Committer(Base):
    """
    Represents a GitHub committer
    """
    __tablename__ = 'committers'
    
    id = Column(Integer, primary_key=True)
    login = Column(String(255), unique=True, nullable=True)  
    name = Column(String(255), nullable=True)  
    email = Column(String(255), nullable=True)
    avatar_url = Column(String(255), nullable=True)
    

    commits = relationship("Commit", back_populates="committer")
    
    def __repr__(self):
        return f"<Committer(login='{self.login}', name='{self.name}')>"

class Author(Base):
    """
    Represents a GitHub author
    """
    __tablename__ = 'authors'
    
    id = Column(Integer, primary_key=True)
    login = Column(String(255), unique=True, nullable=True)  
    name = Column(String(255), nullable=True)  
    email = Column(String(255), nullable=True)
    avatar_url = Column(String(255), nullable=True)
    

    commits = relationship("Commit", back_populates="author")
    
    def __repr__(self):
        return f"<Author(login='{self.login}', name='{self.name}')>"

class Commit(Base):
    """
    Represents a GitHub commit
    """
    __tablename__ = 'commits'
    
    id = Column(Integer, primary_key=True)
    sha = Column(String(40), unique=True, nullable=False)  
    message = Column(Text, nullable=True)  
    committed_at = Column(DateTime, nullable=False)  
    authored_at = Column(DateTime, nullable=True)  
    

    repository_id = Column(Integer, ForeignKey('repositories.id'), nullable=False)
    committer_id = Column(Integer, ForeignKey('committers.id'), nullable=False)
    author_id = Column(Integer, ForeignKey('authors.id'), nullable=False)
    

    repository = relationship("Repository", back_populates="commits")
    committer = relationship("Committer", back_populates="commits")
    author = relationship("Author", back_populates="commits")
    
    def __repr__(self):
        return f"<Commit(sha='{self.sha[:7]}', message='{self.message[:30] if self.message else ''}...')>"