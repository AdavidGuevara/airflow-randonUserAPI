from sqlalchemy.orm import sessionmaker
import pandas as pd
import sqlalchemy
import requests
import sqlite3


def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No user downloaded. Finishing execution")
        return False

    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def run_user_etl():
    DATABASE_LOCATION = "sqlite:///users.sqlite"

    # Extract data:
    response = requests.get("https://randomuser.me/api/")
    if response.status_code == 200:
        payload = response.json()
        result = payload.get("results", [])
        for user in result:
            firstname = user["name"]["first"]
            lastname = user["name"]["last"]
            gender = user["gender"]
            age = user["dob"]["age"]
            country = user["location"]["country"]
            city = user["location"]["city"]
            email = user["email"]

    else:
        raise Exception("Connection failed.")

    # Transform data:
    user_info = {
        "firstname": firstname.lower(),
        "lastname": lastname.lower(),
        "gender": gender.lower(),
        "age": int(age),
        "country": country.lower(),
        "city": city.lower(),
        "email": email.lower(),
    }

    users = pd.DataFrame(
        user_info,
        columns=["firstname", "lastname", "gender", "age", "country", "city", "email"],
        index=[0],
    )

    if check_if_valid_data(users):
        print("Data valid, proceed to Load stage")

    # load data:
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
    print("Opened database successfully")

    try:
        users.to_sql("users", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")
