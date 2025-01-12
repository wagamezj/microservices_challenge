# services/backup_restore.py

from fastavro import writer, reader, parse_schema
from sqlalchemy.orm import Session
import os
from models import Department, Job, HiredEmployee

# Ejemplo de esquemas Avro para cada tabla
department_schema = {
    "name": "department_record",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "department", "type": "string"},
    ]
}

job_schema = {
    "name": "job_record",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "job", "type": "string"},
    ]
}

hired_employee_schema = {
    "name": "hired_employee_record",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "datetime", "type": "string"},
        {"name": "department_id", "type": "int"},
        {"name": "job_id", "type": "int"},
    ]
}


def backup_table(table_name: str, db: Session, backup_dir="backups"):
    """
    Genera un archivo Avro de la tabla especificada y lo almacena en backup_dir.
    """
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)

    if table_name == "departments":
        records = db.query(Department).all()
        parsed_schema = parse_schema(department_schema)
        avro_file = os.path.join(backup_dir, "departments.avro")
        # Convertimos los objetos en dict
        dict_records = [{"id": r.id, "department": r.department} for r in records]

    elif table_name == "jobs":
        records = db.query(Job).all()
        parsed_schema = parse_schema(job_schema)
        avro_file = os.path.join(backup_dir, "jobs.avro")
        dict_records = [{"id": r.id, "job": r.job} for r in records]

    elif table_name == "hired_employees":
        records = db.query(HiredEmployee).all()
        parsed_schema = parse_schema(hired_employee_schema)
        avro_file = os.path.join(backup_dir, "hired_employees.avro")
        dict_records = [{
            "id": r.id,
            "name": r.name,
            "datetime": r.datetime.isoformat(),
            "department_id": r.department_id,
            "job_id": r.job_id
        } for r in records]

    else:
        raise ValueError("Tabla no soportada para backup.")

    with open(avro_file, 'wb') as out:
        writer(out, parsed_schema, dict_records)

    return avro_file


def restore_table(table_name: str, db: Session, backup_file: str):
    """
    Restaura los datos de la tabla desde un archivo Avro.
    """
    if table_name == "departments":
        parsed_schema = parse_schema(department_schema)
        table_class = Department
    elif table_name == "jobs":
        parsed_schema = parse_schema(job_schema)
        table_class = Job
    elif table_name == "hired_employees":
        parsed_schema = parse_schema(hired_employee_schema)
        table_class = HiredEmployee
    else:
        raise ValueError("Tabla no soportada para restore.")

    # Vaciar la tabla primero (opcional, depende de la lógica)
    db.query(table_class).delete()
    db.commit()

    with open(backup_file, 'rb') as fp:
        avro_reader = reader(fp, return_record_name=False)
        for record in avro_reader:
            if table_name == "departments":
                obj = Department(**record)
            elif table_name == "jobs":
                obj = Job(**record)
            elif table_name == "hired_employees":
                # Convertir string a datetime
                record["datetime"] = record["datetime"]  # Podríamos parsearlo si se requiere
                obj = HiredEmployee(**record)
            db.add(obj)

    db.commit()


    
