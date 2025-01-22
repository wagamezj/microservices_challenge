from pydantic import BaseModel, Field
from datetime import datetime
from typing import List

class HiredEmployeeCreate(BaseModel):
    id: int
    name: str
    datetime: datetime
    department_id: int
    job_id: int

class DepartmentCreate(BaseModel):
    id: int
    department: str

class JobCreate(BaseModel):
    id: int
    job: str


class BatchData(BaseModel):
    hired_employees: List[HiredEmployeeCreate] = Field(default_factory=list)
    departments: List[DepartmentCreate] = Field(default_factory=list)
    jobs: List[JobCreate] = Field(default_factory=list)


