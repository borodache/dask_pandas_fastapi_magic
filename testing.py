import pandas as pd
import unittest

from data import Data


class TestDaskMethod(unittest.TestCase):
    def test_grouping_and_average(self):
        df_input = pd.DataFrame({"timestamp": ["2000-01-01 00:00:00", "2000-01-02 00:00:00",
                                               "2000-01-03 00:00:00", "2000-01-04 00:00:00"],
                                 "id": [1, 2, 3, 4],
                                 "name": ["Alex", "Alex", "Henn", "Henn"],
                                 "x": [2, 4, 3, 5],
                                "y": [8, 8, 8, 8]
        })
        df_output = pd.DataFrame({"timestamp": ["2000-01-01 00:00:00", "2000-01-02 00:00:00",
                                               "2000-01-03 00:00:00", "2000-01-04 00:00:00"],
                                 "id": [1, 2, 3, 4],
                                 "name": ["Alex", "Alex", "Henn", "Henn"],
                                 "x": [2, 4, 3, 5],
                                 "y": [8, 8, 8, 8],
                                 "x_mean": [3, 3, 4, 4]
                                 })
        data = Data()
        data.df = df_input
        data.grp_name_avg_x()
        # print(data.df)
        pd.testing.assert_frame_equal(data.df, df_output)

    def test_top10x_in_name(self):
        name = "Alex"
        df_input = pd.DataFrame({"timestamp": ["2000-01-01 00:00:00", "2000-01-02 00:00:00",
                                               "2000-01-03 00:00:00", "2000-01-04 00:00:00"],
                                 "id": [1, 2, 3, 4],
                                 "name": ["Alex", "Alex", "Henn", "Henn"],
                                 "x": [2, 4, 3, 5],
                                "y": [8, 8, 8, 8]
        })
        df_output = pd.DataFrame({"timestamp": ["2000-01-02 00:00:00", "2000-01-01 00:00:00"],
                                 "id": [2, 1],
                                 "name": ["Alex", "Alex"],
                                 "x": [4, 2],
                                 "y": [8, 8],
                                 "x_mean": [3, 3]
                                 })
        data = Data()
        data.df = df_input
        pd.testing.assert_frame_equal(data.get_name_top_10(name).reset_index(drop=True), df_output.reset_index(drop=True))

    def test_top10x_in_name_twice(self):
        name = "Alex"
        df_input = pd.DataFrame({"timestamp": ["2000-01-01 00:00:00", "2000-01-02 00:00:00",
                                               "2000-01-03 00:00:00", "2000-01-04 00:00:00"],
                                 "id": [1, 2, 3, 4],
                                 "name": ["Alex", "Alex", "Henn", "Henn"],
                                 "x": [2, 4, 3, 5],
                                "y": [8, 8, 8, 8]
        })
        df_output = pd.DataFrame({"timestamp": ["2000-01-02 00:00:00", "2000-01-01 00:00:00"],
                                 "id": [2, 1],
                                 "name": ["Alex", "Alex"],
                                 "x": [4, 2],
                                 "y": [8, 8],
                                 "x_mean": [3, 3]
                                 })
        data = Data()
        data.df = df_input
        data.get_name_top_10(name)
        pd.testing.assert_frame_equal(data.get_name_top_10(name).reset_index(drop=True), df_output.reset_index(drop=True))
