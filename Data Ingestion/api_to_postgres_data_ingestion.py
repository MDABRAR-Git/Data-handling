import requests
import psycopg2

# API endpoint
URL = "https://jsonplaceholder.typicode.com/posts"

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="ingestion_db",
    user="postgres",
    password="your_password_here",
    port=5432
)

cur = conn.cursor()

# Fetch data from API
response = requests.get(URL)

if response.status_code != 200:
    raise Exception("Failed to fetch data from API")

data = response.json()

# Insert into database
for row in data:
    cur.execute(
        """
        INSERT INTO posts (user_id, id, title, body)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (row["userId"], row["id"], row["title"], row["body"])
    )

conn.commit()

cur.close()
conn.close()

print("Data successfully ingested.")
