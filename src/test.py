import unittest
from unittest.mock import patch
from bronze import DataManager

class TestDataManager(unittest.TestCase):
    @patch('duckdb_manager.DuckDBManager')
    @patch('aws_manager.AWSManager')
    def test_load_and_transform_data(self, MockAWSManager, MockDuckDBManager):
        # Setup
        db_manager = MockDuckDBManager()
        aws_manager = MockAWSManager()
        data_manager = DataManager(db_manager, aws_manager, "/fake/path", "fake_bucket")

        # Execute
        data_manager.load_and_transform_data("/path/to/fake.json", "test_table")

        # Assert
        expected_query = "\n            INSERT INTO test_table\n            SELECT * FROM read_json_auto('/path/to/fake.json')\n            WHERE id NOT IN (SELECT id FROM test_table);\n            "
        db_manager.execute_query.assert_called_with(expected_query)

if __name__ == '__main__':
    unittest.main()
