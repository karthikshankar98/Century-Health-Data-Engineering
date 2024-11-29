import sqlite3

# Path to your SQLite database file
db_file = "century_health.db"


def query_database(query):

    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)
        results = cursor.fetchall()

        # Close the connection
        connection.close()

        return results

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None


if __name__ == "__main__":
    # Example: Replace this query with what you need
    sql_query = "SELECT * FROM patients_table LIMIT 10;"
  #  sql_query = "SELECT * FROM conditions_table LIMIT 10;"
  #  sql_query = "SELECT * FROM encounters_table LIMIT 10;"
  #  sql_query = "SELECT * FROM medications_table LIMIT 10;"
  #  sql_query = "SELECT * FROM symptoms_table LIMIT 10;"

    # Fetch the query results
    results = query_database(sql_query)

    if results:
        print("Query Results:")
        for row in results:
            print(row)
    else:
        print("No results or error occurred.")
