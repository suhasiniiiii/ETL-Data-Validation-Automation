# ETL-Data-Validation-Automation

This project demonstrates automated testing of ETL logic where:
- Two tables (`users`, `addresses`) are merged
- `full_name` and `location` fields are generated

## 💡 Validations:
- Full name = `first_name + last_name`
- Location = `city + country`

## 🔧 How to Run

python etl_simulation.py
python tests/test_merge_validation.py
