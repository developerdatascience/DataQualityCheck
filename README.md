# DataQualityCheck

DataQualityChecker is a Python package for performing data quality checks on PySpark DataFrames. It provides a comprehensive set of checks to validate the integrity, consistency, and accuracy of data before processing, ensuring that data meets the required standards. The package is ideal for ETL workflows, data pipelines, and any scenario where high-quality data is essential.

### Features
* Null Value Check: Ensures that specified columns do not contain null values.
* Uniqueness Check: Verifies that the values in a specified column are unique.
* Range Check: Checks if values in a column fall within a specified range.
* Valid Values Check: Confirms that values in a column belong to a list of predefined valid values.
* Schema Validation: Validates the presence of required columns and their data types.

## Installation
To install DataQualityChecker, clone the repository and install dependencies:

```bash
pip install -r requirements.txt
```

## Usage
Here’s how to use DataQualityChecker:

1. Initialize SparkSession:

   ```bash
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.appName("DataQualityCheckerExample").getOrCreate()
   ```

2. Define DataFrame and Expected Schema:
   from pyspark.sql.types import StructType, StructField, IntegerType, StringType

   ```bash
   data = [(1, "John", 25, "M"), (2, "Jane", None, "F"), (3, "Alice", 35, None)]
   schema = StructType([
      StructField("id", IntegerType(), True),
      StructField("name", StringType(), True),
      StructField("age", IntegerType(), True),
      StructField("gender", StringType(), True)
    ])
   df = spark.createDataFrame(data, schema)
    
   expected_schema = {
      "id": IntegerType(),
      "name": StringType(),
      "age": IntegerType(),
      "gender": StringType()
    }
   ```
3. Initialize DataQualityChecker:
   ```bash
   from data_quality_checker import DataQualityChecker

   dq_checker = DataQualityChecker(df, expected_schema)
   ```

4. Run Checks:
   ```bash
   dq_checker.check_null_values()
   dq_checker.check_uniqueness("id")
   dq_checker.check_value_range("age", 20, 40)
   dq_checker.check_valid_values("gender", ["M", "F"])
   dq_checker.check_column_presence()
   dq_checker.check_column_data_types()
    
   results_df = dq_checker.run_checks()
   results_df.show(truncate=False)
   ```

## Example Output

The output will be a DataFrame containing a summary of each check, with details on whether the check passed or failed:

| Check                           | Passed | Details                            |
|---------------------------------|--------|------------------------------------|
| Null check on column age        | False  | 1 null value found                |
| Uniqueness check on column id   | True   | All values are unique             |
| Range check for column age      | True   | All values within range           |
| Valid values check for column gender | False | 1 invalid value found      |
| Schema column presence check    | True   | All necessary columns are present |
| Data type check for column age  | True   | Data type matches                 |


## Running Tests
Run unit tests with unittest to verify the integrity of the DataQualityChecker:
```bash
python -m unittest discover
```



 



  



