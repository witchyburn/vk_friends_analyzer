#!/bin/bash
set -e

echo "Starting database initialization"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $AIRFLOW_DB_USER WITH PASSWORD '$AIRFLOW_DB_PASSWORD';
    
    CREATE DATABASE $AIRFLOW_DB_NAME OWNER $AIRFLOW_DB_USER;

    GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB_NAME TO $AIRFLOW_DB_USER;

    CREATE USER $METABASE_DB_USER WITH PASSWORD '$METABASE_DB_PASSWORD';
    
    CREATE DATABASE $METABASE_DB_NAME OWNER $METABASE_DB_USER;
    
    GRANT ALL PRIVILEGES ON DATABASE $METABASE_DB_NAME TO $METABASE_DB_USER;

    \c $POSTGRES_DB $POSTGRES_USER;
    
    CREATE TABLE IF NOT EXISTS statistics (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        sex VARCHAR(1),
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        city VARCHAR(30),
        university VARCHAR(100),
        last_seen TIMESTAMPTZ,
        relation VARCHAR(50),
        friends INTEGER,
        followers INTEGER,
        audios INTEGER,
        videos INTEGER,
        job_place VARCHAR(100),
        birth_date DATE,
        age INTEGER,
        birth_month VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
EOSQL

echo "Database initialization completed successfully!"