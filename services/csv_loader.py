# services/csv_loader.py

import csv
from datetime import datetime
from sqlalchemy.orm import Session
from models import HiredEmployee, Department, Job

def load_hired_employees_csv(file_path: str, db: Session):
    """ Carga CSV en la tabla hired_employees """
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            try:
                emp_id = int(row[0])
                name = row[1].strip()
                dt = datetime.fromisoformat(row[2].replace("Z", ""))
                department_id = int(row[3])
                job_id = int(row[4])

                hired_emp = HiredEmployee(
                    id=emp_id,
                    name=name,
                    datetime=dt,
                    department_id=department_id,
                    job_id=job_id
                )
                db.add(hired_emp)
            except Exception as e:
                print(f"Error en fila {row}: {e}")

    db.commit()


def load_departments_csv(file_path: str, db: Session):
    """ Carga CSV en la tabla departments """
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            try:
                dept_id = int(row[0])
                department_name = row[1].strip()
                dept = Department(id=dept_id, department=department_name)
                db.add(dept)
            except Exception as e:
                print(f"Error en fila {row}: {e}")
    db.commit()


def load_jobs_csv(file_path: str, db: Session):
    """ Carga CSV en la tabla jobs """
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            try:
                job_id = int(row[0])
                job_name = row[1].strip()
                job = Job(id=job_id, job=job_name)
                db.add(job)
            except Exception as e:
                print(f"Error en fila {row}: {e}")
    db.commit()

def load_csv(table_name: str, file_path: str, db: Session):
    """
    Función genérica para cargar el CSV en la tabla indicada por table_name.
    """
    if table_name.lower() == "hired_employees":
        load_hired_employees_csv(file_path, db)
    elif table_name.lower() == "departments":
        load_departments_csv(file_path, db)
    elif table_name.lower() == "jobs":
        load_jobs_csv(file_path, db)
    else:
        raise ValueError(f"Tabla '{table_name}' no soportada.")
