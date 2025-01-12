from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship, declarative_base
from database import engine

Base = declarative_base()

class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True, index=True)
    department = Column(String, nullable=False)

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, index=True)
    job = Column(String, nullable=False)

class HiredEmployee(Base):
    __tablename__ = "hired_employees"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    datetime = Column(DateTime, nullable=False)
    department_id = Column(Integer, ForeignKey("departments.id"), nullable=False)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    
    department = relationship("Department", backref="employees")
    job = relationship("Job", backref="employees")


"""def create_tables():
    Base.metadata.create_all(bind=engine)"""