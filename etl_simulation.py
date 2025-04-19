import sqlite3
import pandas as pd

# Load CSVs
users_df = pd.read_csv("data/users.csv")
addresses_df = pd.read_csv("data/addresses.csv")

# Merge data and transform
final_df = pd.merge(users_df, addresses_df, left_on="id", right_on="user_id")
final_df["full_name"] = final_df["first_name"] + " " + final_df["last_name"]
final_df["location"] = final_df["city"] + ", " + final_df["country"]

# Final table structure
final_df = final_df[["id", "full_name", "location"]]

# Save to SQLite
conn = sqlite3.connect("db/etl_test.db")
final_df.to_sql("final_table", conn, if_exists="replace", index=False)
conn.close()
