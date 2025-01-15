-- Crear tabla de departamentos
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    department VARCHAR(255) NOT NULL
);

-- Crear tabla de trabajos
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job VARCHAR(255) NOT NULL
);

-- Crear tabla de empleados contratados
CREATE TABLE hired_employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    department_id INTEGER NOT NULL REFERENCES departments(id),
    job_id INTEGER NOT NULL REFERENCES jobs(id)
);