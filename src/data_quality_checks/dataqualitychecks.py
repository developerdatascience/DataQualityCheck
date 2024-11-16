from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType
from typing import Dict, List, Union, Any

from dataclasses import dataclass, field


spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

@dataclass
class DataQualityCheck:
    """
    A class to perform data quality checks on a PySpark DataFrame.

    Attributes:
    - df (DataFrame): The PySpark DataFrame to perform checks on.
    - expected_schema (dict): A dictionary of column names and expected data types.
    - results (list): A list to store the results of each data quality check.
    """
    df: DataFrame
    expected_schema: Dict[str, DataType]
    results: List[Dict[str, Any]] = field(default_factory=list, init=False)

    def check_null_values(self):
            """Checks for null values in each column."""
            for column in self.df.columns:
                null_count = self.df.filter(col(column).isNull()).count()
                result = {
                    "Check": f"Null check on column {column}",
                    "Passed": null_count == 0,
                    "Details": f"{null_count} null values found" if null_count > 0 else "No null values"
                }
                self.results.append(result)

    def check_uniqueness(self, column: str):
        """Checks for uniqueness in a specified column."""
        if column in self.df.columns:
            duplicate_count = self.df.groupBy(column).count().filter(col("count") > 1).count()
            result = {
                "Check": f"Uniqueness check on column {column}",
                "Passed": duplicate_count == 0,
                "Details": f"{duplicate_count} duplicate values found" if duplicate_count > 0 else "All values are unique"
            }
            self.results.append(result)

    def check_value_range(self, column: str, min_val: int, max_val: int):
        """Checks if values in a column fall within a specified range."""
        if column in self.df.columns:
            invalid_count = self.df.filter((col(column) < min_val) | (col(column) > max_val)).count()
            result = {
                "Check": f"Range check for column {column} ({min_val} <= value <= {max_val})",
                "Passed": invalid_count == 0,
                "Details": f"{invalid_count} values out of range" if invalid_count > 0 else "All values within range"
            }
            self.results.append(result)

    def check_valid_values(self, column: str, valid_values: List[Any]):
        """Checks if values in a column are within a list of valid values."""
        if column in self.df.columns:
            invalid_count = self.df.filter(~col(column).isin(valid_values)).count()
            result = {
                "Check": f"Valid values check for column {column}",
                "Passed": invalid_count == 0,
                "Details": f"{invalid_count} invalid values found" if invalid_count > 0 else "All values are valid"
            }
            self.results.append(result)

    def check_column_presence(self):
        """Checks if all necessary columns are present in the DataFrame."""
        missing_columns = [col for col in self.expected_schema if col not in self.df.columns]
        result = {
            "Check": "Schema column presence check",
            "Passed": len(missing_columns) == 0,
            "Details": f"Missing columns: {', '.join(missing_columns)}" if missing_columns else "All necessary columns are present"
        }
        self.results.append(result)

    def check_column_data_types(self):
        """Checks if each column has the correct data type as per the expected schema."""
        for column, expected_type in self.expected_schema.items():
            if column in self.df.columns:
                actual_type = self.df.schema[column].dataType
                type_check_passed = isinstance(actual_type, type(expected_type))
                result = {
                    "Check": f"Data type check for column {column}",
                    "Passed": type_check_passed,
                    "Details": f"Expected: {expected_type}, Actual: {actual_type}" if not type_check_passed else "Data type matches"
                }
                self.results.append(result)

    def run_checks(self):
        """Runs all the data quality checks and returns a summary DataFrame."""
        # Run individual checks
        self.check_null_values()
        self.check_column_presence()
        self.check_column_data_types()

        # Convert the results to a DataFrame
        results_df = self.df.sparkSession.createDataFrame(self.results)
        return results_df


if __name__ == "__main__":
    DataQualityCheck()