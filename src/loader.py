import psycopg2
from config import DB_CONFIG

def load_to_db(df):

    records = df.collect()

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        for row in records:
            cursor.execute("""
                INSERT INTO fact_transactions (
                    transaction_id,
                    user_id,
                    transaction_time,
                    amount,
                    status
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id)
                DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    transaction_time = EXCLUDED.transaction_time,
                    amount = EXCLUDED.amount,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                WHERE fact_transactions.transaction_time < EXCLUDED.transaction_time;
            """, (
                row["transaction_id"],
                row["user_id"],
                row["transaction_time"],
                row["amount"],
                row["status"]
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("Error during load:", e)
        raise

    finally:
        cursor.close()
        conn.close()

    return len(records)