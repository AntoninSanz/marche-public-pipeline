import psycopg2


def connect_to_postgres():
    conn = psycopg2.connect(database="marches_publiques",
                            user="antoninsanzovo",
                            host='localhost',
                            password="",
                            port='5432')
    return conn


def load_data_to_postgres(connection, staging_file_path):
    cur = connection.cursor()
    try:
        print("Connected to the database")
        staging_file_path = "staging/decp.csv"
        with open(staging_file_path, 'r', encoding='utf-8') as f:
            cur.copy_expert(
                """
                        COPY raw_marches(id_marche, date_notification, objet_marche, montant_marche, id_acheteur, code_postal_acheteur)
                        FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
                        """, f
            )
        connection.commit()
        print("✅ Data loaded successfully into raw_marches")

    except Exception as e:
        connection.rollback()
        print("❌ Error while loading data into Postgres")
        print("➡️", e)


if __name__ == "__main__":

    conn = connect_to_postgres()
    load_data_to_postgres(conn, "staging/decp.csv")
    conn.close()
