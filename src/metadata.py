import psycopg2
from config import DB_CONFIG


def is_file_processed(file_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM processed_files WHERE file_name = %s;",
        (file_name,)
    )

    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return result is not None


def mark_file_processed(file_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute(
        "INSERT INTO processed_files (file_name) VALUES (%s);",
        (file_name,)
    )

    conn.commit()
    cursor.close()
    conn.close()