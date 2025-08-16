import pytest
import pandas as pd
from main import process_csv_data

class TestProcessCsvData:
    """Test para la función  process_csv_data"""
    def test_basic_csv_processing(self):
        csv_content = """name,age,salary
        jhon,25,50000
        Jane,30,70000
        Bob,35,70000"""

        result = process_csv_data(csv_content)

        assert result['row_count'] == 3
        assert result['column_count'] == 3
        assert result['columns'] == ['name', 'age', 'salary']
        assert result['numeric_columns'] == ['age', 'salary']
        assert result['has_missing_data'] == False
        assert result['total_missing'] == 0