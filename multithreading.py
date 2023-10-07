import threading
import pandas as pd
import MySQLdb as mysql

# Define your database connection parameters
database_host = "localhost"
database_user = "root"
database_password = "Mysql!@#123"
database_name = "sakila"

# Create a database connection
conn = mysql.connect(host=database_host, user=database_user, password=database_password, database=database_name)

# Create a cursor for executing SQL queries
cursor = conn.cursor()

# Define the fetch_data function
def fetch_data(query, result_list):
    cursor.execute(query)
    data = cursor.fetchall()
    result_list.append(data)

# Define your list of queries
queries = [
    "SELECT first_name, last_name FROM actor_info",
    "SELECT title, description FROM film",
    # Add more queries as needed
]

# Create a list to store the results
results = []

# Create threads for each query
threads = []
for query in queries:
    thread = threading.Thread(target=fetch_data, args=(query, results))
    threads.append(thread)

# Start the threads
for thread in threads:
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

# Iterate over the queries and results
for query, result in zip(queries, results):
    print(f"Data from query: {query}")
    print("Results:")
    if result:
        # Specify column names directly based on the query structure
        if "actor_info" in query:
            column_names = ["first_name", "last_name"]
        elif "film" in query:
            column_names = ["title", "description"]
        # Add more cases as needed for additional queries
        else:
            column_names = []

        # Convert the query result to a DataFrame with column names
        df = pd.DataFrame(result, columns=column_names)
        # Print the DataFrame
        print(df)
        # Save the DataFrame to a CSV file
        df.to_csv(f"{query.replace(' ', '_')}.csv", index=False)
    else:
        print("No results found for this query")


