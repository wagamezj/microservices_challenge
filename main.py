# main.py

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session 
from sqlalchemy import text
from database import SessionLocal
from schemas import BatchData
from models import Department, Job, HiredEmployee
from services.csv_loader import load_csv
from services.backup_restore import backup_table, restore_table
from typing import Optional
import pandas as pd


app = FastAPI(title="Servicio de Migracion e Ingesta de Datos Prueba Globant")

# Dependencia para obtener la sesión de base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def root():
    return {"message": "Welcome to Globant's data migration and ingestion services"}


@app.post("/batch_insert", tags=["Data Insertion"])
def batch_insert(batch_data: BatchData, db: Session = Depends(get_db)):
    """
    Inserts data into the departments, jobs, and hired employees tables. 1 to 1000 rows are allowed per list. 
    Rows that do not meet the criteria are not inserted.
    """
    # Validación de tamaño de lote
    if len(batch_data.departments) > 1000 or len(batch_data.jobs) > 1000 or len(batch_data.hired_employees) > 1000:
        raise HTTPException(status_code=400, detail="Cada lista debe tener máximo 1000 registros.")
    
    # Insertar departments
    for dept in batch_data.departments:
        try:
            new_dept = Department(
                id=dept.id,
                department=dept.department
            )
            db.add(new_dept)
        except Exception as e:
            print(f"Error en departmento favor verificar {dept.dict()}: {e}")
    
    # Insertar jobs
    for job in batch_data.jobs:
        try:
            new_job = Job(
                id=job.id,
                job=job.job
            )
            db.add(new_job)
        except Exception as e:
            print(f"Error en job {job.dict()}: {e}")

    # Insertar hired_employees
    for emp in batch_data.hired_employees:
        try:
            new_emp = HiredEmployee(
                id=emp.id,
                name=emp.name,
                datetime=emp.datetime,
                department_id=emp.department_id,
                job_id=emp.job_id
            )
            db.add(new_emp)
        except Exception as e:
            print(f"Error en hired_employee {emp.dict()}: {e}")

    db.commit()
    return {"message": "Data inserted successfully."}


@app.post("/load_csv",tags=["Data Insertion"])
def load_csv_endpoint(table_name: str, file_path: str, db: Session = Depends(get_db)):
    """
    Endpoint to upload a CSV file to the specified table.

        Params:
        - table_name: Name of the table ("hired_employees", "departments", "jobs")
        - file_path: Full path of the CSV file to upload.

        Example call:
        POST /load_csv?table_name=departments&file_path=./data/departments.csv
    """
    try:
        load_csv(table_name, file_path, db)
        return {"message": f"Datos de '{table_name}' cargados correctamente desde {file_path}."}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")
    

@app.get("/backup/{table_name}", tags=["Backup"])
def backup(table_name: str, db: Session = Depends(get_db)):
    """
    Endpoint to backup a table in AVRO format.
    """
    try:
        file_path = backup_table(table_name, db)
        return {"message": f"Backup de {table_name} creado en {file_path}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/restore/{table_name}", tags=["Backup"])
def restore(table_name: str, backup_file: str, db: Session = Depends(get_db)):
    """
    Endpoint to restore a table from an AVRO file.
    """
    try:
        restore_table(table_name, db, backup_file)
        return {"message": f"Tabla {table_name} restaurada desde {backup_file}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/Quarterly_hires", tags=['Query'])
def get_quarterly_hires_pandas(db: Session = Depends(get_db)):

    """
    Returns the number of employees hired by each job and department in 2021,
    divided by quarter (Q1, Q2, Q3, Q4), sorted alphabetically by department and job.
    """
    query_sql = text("""
        SELECT 
            d.department AS department, 
            j.job AS job,
            EXTRACT(QUARTER FROM he.datetime) AS quarter,
            COUNT(*) AS hires
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
        GROUP BY d.department, j.job, EXTRACT(QUARTER FROM he.datetime)
        ORDER BY d.department, j.job;
    """)

    result = db.execute(query_sql).fetchall()
    # result -> lista de filas con (department, job, quarter, hires)

    # Vamos a "pivotear" los datos para tener Q1, Q2, Q3, Q4 en columnas
    # Estructura { (dep, job): {1: X, 2: Y, 3: Z, 4: W} }
    quarterly_dict = {}

    for row in result:
        department = row.department
        job = row.job
        quarter = int(row.quarter)
        hires = row.hires

        if (department, job) not in quarterly_dict:
            quarterly_dict[(department, job)] = {1: 0, 2: 0, 3: 0, 4: 0}
        
        quarterly_dict[(department, job)][quarter] = hires

    # Convertimos esa estructura en una lista de diccionarios ordenados
    response = []
    # Orden alfabético por department y luego job
    for (dep, job), quarters in sorted(quarterly_dict.items(), key=lambda x: (x[0][0], x[0][1])):
        response.append({
            "department": dep,
            "job": job,
            "Q1": quarters[1],
            "Q2": quarters[2],
            "Q3": quarters[3],
            "Q4": quarters[4]
        })

    return response

@app.get("/departments_above_mean", tags=['Query'])
def get_departments_above_mean_pandas(db: Session = Depends(get_db)):
    """
   
    Returns the departments that hired more employees than the 2021 average,
    sorted in descending order by the number of employees (hired).
    """ 
    query_sql = text("""
        SELECT 
            d.id AS id,
            d.department AS department,
            COUNT(*) AS hired
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
        GROUP BY d.id, d.department
        -- ORDER BY hired DESC  <-- Podríamos ordenar aquí, pero lo haremos luego en Python.
    """)

    rows = db.execute(query_sql).fetchall()
    # rows -> lista de filas con (id, department, hired)

    if not rows:
        return []

    # Calculamos el promedio (mean) de 'hired' en todos los departamentos
    total_hired = sum(r.hired for r in rows)
    mean_val = total_hired / len(rows) if len(rows) > 0 else 0

    # Filtramos aquellos cuyo 'hired' > mean_val
    above_mean = [r for r in rows if r.hired > mean_val]

    # Ordenamos descendentemente por hired
    above_mean_sorted = sorted(above_mean, key=lambda x: x.hired, reverse=True)

    # Armamos la respuesta
    response = []
    for r in above_mean_sorted:
        response.append({
            "id": r.id,
            "department": r.department,
            "hired": r.hired
        })

    return response
    return final_result









