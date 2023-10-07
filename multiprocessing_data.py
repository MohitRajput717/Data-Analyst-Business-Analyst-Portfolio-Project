import multiprocessing
import pandas as pd
from sqlalchemy import create_engine, text

# Define your database URL without the password
database_url = "mysql+mysqldb://root@localhost:3306/sakila"

# Create a database engine and specify the password separately
engine = create_engine(database_url, connect_args={"password": "Mysql!@#123"})

# Define the fetch_data function
def fetch_data(query):
    with engine.connect() as connection:
        # Use text() to create a SQLAlchemy text object for the query
        query = text(query)
        result = connection.execute(query)
        data = result.fetchall()
    return data

# Define your list of queries
queries = [
    "select first_name, last_name from actor_info",
    "select title, description from film",
    "select category_id, name from category"  # Add your third query here
    # Add more queries as needed
]

if __name__ == "__main__":
    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(fetch_data, queries)

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
            elif "category" in query:
                column_names = ["category_id", "name"]
            else:
                column_names = []  # Add more cases as needed

            # Convert the query result to a DataFrame with column names
            df = pd.DataFrame(result, columns=column_names)
            # Save the DataFrame to a CSV file
            df.to_csv(f"{query.replace(' ', '_')}.csv", index=False)
        else:
            print("No results found for this query")
