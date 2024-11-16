import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.data_quality_checks import dataqualitychecks

class TestDataQualityCheck(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("DataQualityCheckerTest").getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Sample data and schema
        data = [
            (1, "John", 25, "M"),
            (2, "Jane", None, "F"),
            (3, "Alice", 35, None),
            (4, "Bob", 30, "M"),
            (4, "Bob", 30, "M")  # Duplicate row for testing uniqueness
        ]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True)
        ])
        self.df = self.spark.createDataFrame(data, schema)
        
        # Expected schema
        self.expected_schema = {
            "id": IntegerType(),
            "name": StringType(),
            "age": IntegerType(),
            "gender": StringType()
        }
        
        # Initialize DataQualityChecker
        self.dq_checker = dataqualitychecks.DataQualityCheck(self.df, self.expected_schema)

    def test_check_null_values(self):
        self.dq_checker.check_null_values()
        results = self.dq_checker.results
        null_check_result = next((item for item in results if "Null check" in item["Check"]), None)
        self.assertIsNotNone(null_check_result)
        self.assertFalse(null_check_result["Passed"])
        self.assertIn("null values found", null_check_result["Details"])

    def test_check_uniqueness(self):
        self.dq_checker.check_uniqueness("id")
        results = self.dq_checker.results
        uniqueness_check_result = next((item for item in results if "Uniqueness check on column id" in item["Check"]), None)
        self.assertIsNotNone(uniqueness_check_result)
        self.assertFalse(uniqueness_check_result["Passed"])
        self.assertIn("duplicate values found", uniqueness_check_result["Details"])

    def test_check_value_range(self):
        self.dq_checker.check_value_range("age", 20, 40)
        results = self.dq_checker.results
        range_check_result = next((item for item in results if "Range check for column age" in item["Check"]), None)
        self.assertIsNotNone(range_check_result)
        self.assertTrue(range_check_result["Passed"])
        self.assertIn("All values within range", range_check_result["Details"])

    def test_check_valid_values(self):
        self.dq_checker.check_valid_values("gender", ["M", "F"])
        results = self.dq_checker.results
        valid_values_check_result = next((item for item in results if "Valid values check for column gender" in item["Check"]), None)
        self.assertIsNotNone(valid_values_check_result)
        self.assertFalse(valid_values_check_result["Passed"])
        self.assertIn("invalid values found", valid_values_check_result["Details"])

    def test_check_column_presence(self):
        self.dq_checker.check_column_presence()
        results = self.dq_checker.results
        column_presence_result = next((item for item in results if "Schema column presence check" in item["Check"]), None)
        self.assertIsNotNone(column_presence_result)
        self.assertTrue(column_presence_result["Passed"])

    def test_check_column_data_types(self):
        self.dq_checker.check_column_data_types()
        results = self.dq_checker.results
        data_type_check_result = next((item for item in results if "Data type check" in item["Check"]), None)
        self.assertIsNotNone(data_type_check_result)
        self.assertTrue(data_type_check_result["Passed"])

    def test_run_checks(self):
        # Run all checks
        results_df = self.dq_checker.run_checks()
        results_df.show(truncate=False)

        # Verify that the DataFrame has the expected columns
        self.assertListEqual(results_df.columns, ["Check", "Passed", "Details"])

        # Verify the total number of checks
        self.assertEqual(results_df.count(), len(self.dq_checker.results))


if __name__ == "__main__":
    unittest.main()
