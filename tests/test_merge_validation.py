import sqlite3
import pandas as pd

import sqlite3
import pandas as pd

def test_merged_data():
    # Connect to the SQLite database
    conn = sqlite3.connect("C://Users//Acer//PycharmProjects//ETL-Data-Validation-Automation//db//etl_test.db")
    print("Connected to the database")

    # Load the CSV files
    users_df = pd.read_csv("C://Users//Acer//PycharmProjects//ETL-Data-Validation-Automation//data//users.csv")
    addresses_df = pd.read_csv("C://Users//Acer//PycharmProjects//ETL-Data-Validation-Automation//data//addresses.csv")

    # Fetch the transformed data from the final_table
    transformed_df = pd.read_sql("SELECT * FROM final_table", conn)

    # Debug: Check the first few rows of transformed_df
    print(f"Transformed DataFrame: \n{transformed_df.head()}")

    # Define required columns
    required_columns = ['id', 'full_name', 'location']

    # Check if all required columns are present in the transformed table
    missing_columns = [col for col in required_columns if col not in transformed_df.columns]
    if missing_columns:
        print(f" Missing columns in transformed data: {', '.join(missing_columns)}")
        return

    # Loop through all the ids in the users_df and check if they are present in the transformed table
    for _, row in users_df.iterrows():
        user_id = row['id']
        print(f"Checking ID: {user_id}")

        # Check if ID exists in transformed data
        if user_id not in transformed_df['id'].values:
            print(f"ID {user_id} not found in transformed data.")
            return

        # Get the corresponding transformed row
        transformed_row = transformed_df[transformed_df['id'] == user_id].iloc[0]

        # Validate if the full_name is the concatenation of first_name and last_name
        expected_full_name = f"{row['first_name']} {row['last_name']}"
        if transformed_row['full_name'] != expected_full_name:
            print(f"Full Name mismatch for ID {user_id}. Expected: {expected_full_name}, Found: {transformed_row['full_name']}")
            return

        # Validate if the location is the concatenation of city and country
        address_row = addresses_df[addresses_df['user_id'] == user_id].iloc[0]
        expected_location = f"{address_row['city']}, {address_row['country']}"
        if transformed_row['location'] != expected_location:
            print(f"Location mismatch for ID {user_id}. Expected: {expected_location}, Found: {transformed_row['location']}")
            return

    print("Data transformation and validation passed!")

if __name__ == "__main__":
    test_merged_data()
