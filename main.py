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
    return {"message": "Bienvenido a servicios migración e ingesta de datos de Globant"}


@app.post("/batch_insert", tags=["Data Insertion"])
def batch_insert(batch_data: BatchData, db: Session = Depends(get_db)):
    """
    Inserta datos en las tablas departments, jobs y hired_employees.
    Se permiten de 1 a 1000 filas por lista. Las filas que no cumplan
    con las reglas se loggean y no se insertan.
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
    return {"message": "Datos insertados exitosamente."}


@app.post("/load_csv",tags=["Data Insertion"])
def load_csv_endpoint(table_name: str, file_path: str, db: Session = Depends(get_db)):
    """
    Endpoint para cargar un archivo CSV a la tabla especificada.
    
    Params:
    - table_name: Nombre de la tabla ("hired_employees", "departments", "jobs")
    - file_path: Ruta completa del archivo CSV a cargar.
    
    Ejemplo de llamada:
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
    Endpoint para hacer backup de una tabla en formato AVRO.
    """
    try:
        file_path = backup_table(table_name, db)
        return {"message": f"Backup de {table_name} creado en {file_path}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/restore/{table_name}", tags=["Backup"])
def restore(table_name: str, backup_file: str, db: Session = Depends(get_db)):
    """
    Endpoint para restaurar una tabla a partir de un archivo AVRO.
    """
    try:
        restore_table(table_name, db, backup_file)
        return {"message": f"Tabla {table_name} restaurada desde {backup_file}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/Quarterly_hires", tags=['Query'])
def get_quarterly_hires_pandas(db: Session = Depends(get_db)):
    """
    Retorna el número de empleados contratados por cada job y departamento en 2021,
    dividido por trimestre (Q1, Q2, Q3, Q4), ordenados alfabéticamente por department y job.
    """
    query_sql = text("""
        SELECT d.department,
               j.job,
               he.datetime
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        JOIN jobs j ON he.job_id = j.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
    """)
    rows = db.execute(query_sql).fetchall()
    
    df = pd.DataFrame(rows, columns=["department", "job", "datetime"])
    df["datetime"] = pd.to_datetime(df["datetime"]) 
    df["quarter"] = df["datetime"].dt.quarter

    # 4. Contar hires agrupando por departamento, job, quarter
    #    Equivalente a group by + count(*)
    grouped = df.groupby(["department", "job", "quarter"]).size().reset_index(name="hires")
    # grouped -> DataFrame con columnas [department, job, quarter, hires]

    # 5. Pivotear para tener Q1, Q2, Q3, Q4 en columnas
    pivoted = grouped.pivot_table(
        index=["department","job"], 
        columns="quarter", 
        values="hires", 
        fill_value=0  # si no hubo contrataciones en ese quarter, poner 0
    )

    # 6. Asegurar que las columnas estén en orden Q1, Q2, Q3, Q4
    #    pivoted.columns será algo como Int64Index([1,2,3,4], ...)
    #    Reindexa para asegurar que las columnas existan (si no hubo hires en un quarter, no aparece).
    pivoted = pivoted.reindex(columns=[1,2,3,4], fill_value=0)

    # 7. Renombrar columnas a Q1, Q2, Q3, Q4
    pivoted.columns = ["Q1","Q2","Q3","Q4"]

    # 8. Convertir el index (department, job) a columnas
    pivoted = pivoted.reset_index()

    # 9. Ordenar alfabéticamente por department y job
    pivoted = pivoted.sort_values(by=["department","job"])

    # 10. Convertir a lista de dict para retornar como JSON
    result = pivoted.to_dict(orient="records")

    return result

@app.get("/departments_above_mean", tags=['Query'])
def get_departments_above_mean_pandas(db: Session = Depends(get_db)):
    """
   
    Retorna los departamentos que contrataron más empleados que el promedio de 2021,
    ordenados descendentemente por la cantidad de empleados (hired).
    """
    query_sql = text("""
        SELECT d.id AS department_id,
               d.department AS department,
               he.id AS hired_employee_id,
               he.datetime
        FROM hired_employees he
        JOIN departments d ON he.department_id = d.id
        WHERE EXTRACT(YEAR FROM he.datetime) = 2021
    """)

    rows = db.execute(query_sql).fetchall()
    # rows -> (department_id, department, hired_employee_id, datetime)

    # Convertimos a DataFrame
    df = pd.DataFrame(rows, columns=["department_id","department","hired_employee_id","datetime"])
    # No se necesita dt.quarter aquí, solo necesitamos contar por department
    # Asegurar que datetime sea datetime
    df["datetime"] = pd.to_datetime(df["datetime"])

    # Contar cuántos empleados se contrataron por departamento
    grouped = df.groupby(["department_id","department"]).size().reset_index(name="hired")
    # grouped -> DataFrame con [department_id, department, hired]

    if grouped.empty:
        return []

    # Calcular el promedio global
    mean_val = grouped["hired"].mean()

    # Filtrar los departamentos que superan el promedio
    above_mean = grouped[grouped["hired"] > mean_val]

    # Ordenar descendentemente por hired
    above_mean = above_mean.sort_values(by="hired", ascending=False)

    # Convertir a lista de dict
    result = above_mean.to_dict(orient="records")
    # result -> [{'department_id': X, 'department': 'Name', 'hired': X}, ...]

    # Si quieres devolver la clave como 'id' en vez de 'department_id'
    # renombramos la columna en el DataFrame
    # above_mean.rename(columns={'department_id': 'id'}, inplace=True)
    # O lo transformamos en el diccionario final:
    final_result = []
    for row in result:
        final_result.append({
            "id": row["department_id"],
            "department": row["department"],
            "hired": row["hired"]
        })

    return final_result









