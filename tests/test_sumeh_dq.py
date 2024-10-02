#!/usr/bin/env python

import unittest
import pandas as pd
import os
from unittest.mock import patch
from sumeh_dq.sumeh_dq import quality  # Replace with actual module


class TestQualityFunction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Load the mock data from the CSV file
        cls.mock_data_file = os.path.join(os.path.dirname(__file__), 'mock', 'mock_data.csv')
        cls.mock_config_file = os.path.join(os.path.dirname(__file__), 'mock', 'config_tests.csv')

        # Read the mock data into a pandas DataFrame
        cls.mock_data = pd.read_csv(cls.mock_data_file, sep=';', header=0)

    def test_quality_with_mock_data(self):
        print(self.mock_data)
        result = quality(
            df=self.mock_data,
            source_type='csv',
            file_path=self.mock_config_file,
        )

        print(result)


if __name__ == '__main__':
    unittest.main()
