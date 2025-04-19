import sqlite3
import pandas as pd

def test_merged_data():
    conn = sqlite3.connect("db/etl_test.db")
    df = pd.read_sql("SELECT * FROM final_table", conn)

    expected_data = {
        "id": [1, 2],
        "full_name": ["John Doe", "Jane Smith"],
        "location": ["New York, USA", "London, UK"]
    }
    expected_df = pd.DataFrame(expected_data)

    pd.testing.assert_frame_equal(df.sort_values("id").reset_index(drop=True),
                                  expected_df.sort_values("id").reset_index(drop=True))
    print("✅ Merge validation passed!")

if __name__ == "__main__":
    test_merged_data()
