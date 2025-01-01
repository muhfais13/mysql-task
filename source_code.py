import psycopg2
import csv
import os


def db_connect():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="school_exam_data",
            user="postgres",
            password="root12345",
            port=5433
        )
        print('Connected to the PostgreSQL server.')
        return conn
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def create_table(conn):
    cursor = conn.cursor()
    
    create_table_queries = [
    """CREATE TABLE IF NOT EXISTS student (
        id SERIAL PRIMARY KEY,
        student_id VARCHAR(5),
        student_name VARCHAR(20),
        registered_class VARCHAR(5),
        home_region VARCHAR(10),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """CREATE TABLE IF NOT EXISTS teacher (
        id SERIAL PRIMARY KEY,
        teacher_id VARCHAR(5),
        teacher_name VARCHAR(20),
        subject_id VARCHAR(3),
        subject_name VARCHAR(10),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """, 
    """CREATE TABLE IF NOT EXISTS subject (
        subject_id VARCHAR(3),
        subject_name VARCHAR(10),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """CREATE TABLE IF NOT EXISTS exam_result (
        exam_result_id VARCHAR(5),
        student_id VARCHAR(5),
        subject_id VARCHAR(3),
        exam_date DATE,
        exam_event VARCHAR(10),
        exam_score INT,
        exam_submit_time TIME,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """CREATE TABLE IF NOT EXISTS teacher_mapping (
        id SERIAL PRIMARY KEY,
        subject_id VARCHAR(3),
        class VARCHAR(5),
        teacher_id VARCHAR(5),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    ]
    
    try:
        for query in create_table_queries:
            cursor.execute(query)
        conn.commit()
        print("Tables created successfully!")
        
    except (Exception, psycopg2.DatabaseError) as err:
        conn.rollback()
        print(f"Failed to create table: {err}")
        
    finally:
        cursor.close()


def insert_data_from_csv(conn, csv_file_path, table_name, columns):

    try:
        cursor = conn.cursor()
        
        with open(csv_file_path, 'r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            
            # row_count = 0 
            # data_to_insert = []
            
            # for row in reader:
            #     row_count += 1
            #     data_to_insert.append(row)
            
            # print(f"Total rows read from {os.path.basename(csv_file_path)}: {row_count}")
            
            placeholders = ', '.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            for row in reader:
                cursor.execute(insert_query, row)
                        
        conn.commit()
        print(f"Data from {os.path.basename(csv_file_path)} inserted into {table_name} successfully.")
    
    except Exception as e:
        conn.rollback()
        print(f"Failed to insert data from {os.path.basename(csv_file_path)} into {table_name}: {e}")
    
    finally:
        cursor.close()
        

def main():
    conn = db_connect()
    if conn:
        try:

            create_table(conn)
            
            files_and_tables = [
                {
                    "csv_file": "/Users/muhfais_/Documents/PT. Paragon Corp/Technical Test DE/Technical Test DE - Dataset  - student.csv",
                    "table_name": "student",
                    "columns": ["student_id", "student_name", "registered_class", "home_region"]
                },
                {
                    "csv_file": "/Users/muhfais_/Documents/PT. Paragon Corp/Technical Test DE/Technical Test DE - Dataset  - teacher.csv",
                    "table_name": "teacher",
                    "columns": ["teacher_id", "teacher_name", "subject_id", "subject_name"]
                },
                {
                    "csv_file": "/Users/muhfais_/Documents/PT. Paragon Corp/Technical Test DE/Technical Test DE - Dataset  - exam_result.csv",
                    "table_name": "exam_result",
                    "columns": ["exam_result_id", "student_id", "subject_id", "exam_date", "exam_event", "exam_score", "exam_submit_time"]
                },
                {
                    "csv_file": "/Users/muhfais_/Documents/PT. Paragon Corp/Technical Test DE/Technical Test DE - Dataset  - subject.csv",
                    "table_name": "subject",
                    "columns": ["subject_id", "subject_name"]
                },
                {
                    "csv_file": "/Users/muhfais_/Documents/PT. Paragon Corp/Technical Test DE/Technical Test DE - Dataset  - teacher_mapping.csv",
                    "table_name": "teacher_mapping",
                    "columns": ["subject_id", "class", "teacher_id"]
                }
            ]

            for entry in files_and_tables:
                insert_data_from_csv(conn, entry["csv_file"], entry["table_name"], entry["columns"])
                        
        finally:
            conn.close()


if __name__ == "__main__":
    main()
