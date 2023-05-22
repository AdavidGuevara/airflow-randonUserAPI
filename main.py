import pandas as pd
import sqlalchemy
import requests
import sqlite3


DATABASE_LOCATION = "sqlite:///users.sqlite"


def get_user(url="https://randomuser.me/api/"):
    response = requests.get(url)
    if response.status_code == 200:
        payload = response.json()
        result = payload.get("results", [])
        for user in result:
            return {
                "firstname": user["name"]["first"],
                "lastname": user["name"]["last"],
                "gender": user["gender"],
                "age": user["dob"]["age"],
                "country": user["location"]["country"],
                "city": user["location"]["city"],
                "email": user["email"],
            }
    else:
        print("connection fail.")


def load_data():
    user_info = get_user()
    users = pd.DataFrame(
        user_info,
        columns=["firstname", "lastname", "gender", "age", "country", "city", "email"],
        index=[0],
    )

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("users.sqlite")
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER,
        firstname VARCHAR(100),
        lastname VARCHAR(100),
        gender VARCHAR(50),
        age INTEGER,
        country VARCHAR(100),
        city VARCHAR(100),
        email VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (id)
    )
    """

    cursor.execute(sql_query)
    try:
        users.to_sql("users", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")


if __name__ == "__main__":
    load_data()
