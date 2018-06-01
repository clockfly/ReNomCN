import unittest
from unittest import mock
from renom_cn.pi_interface.interface import PIInterface
import pandas as pd
import numpy as np
from pandas.util.testing import assert_frame_equal
from osisoft.pidevclub.piwebapi.rest import ApiException

GET_PLOT_VALUES_PATH = 'osisoft.pidevclub.piwebapi.api.data_api.DataApi.get_plot_values'
UPDATE_VALUES_PATH = 'osisoft.pidevclub.piwebapi.api.stream_api.StreamApi.update_values_with_http_info'
GET_PATH = 'osisoft.pidevclub.piwebapi.api.attribute_api.AttributeApi.get_by_path'
WEB_ID = 'F1AbEqUVAb-cYwXERFVkVMT1BNRU5UXEVMRU1FTlQyfEFUVFJJQlVURQ'

def mocked_data_get_plot_values(*args, **kwargs):
    if args[0] == 'af:\\\\assetserver_valid\\database_valid\\elements_valid|attributes_valid':
        df = pd.DataFrame({'Good':[True, True, False, True, True],
                           'Questionable': [True, True, False, True, True],
                           'Substituted': [True, True, False, True, True],
                           'Timestamp': pd.date_range('20180401', '20180405'),
                           'Value': [19, 24, 55, 12, 33],
                           'UnitAbbreviation': ['m', 'm', 'm', 'm', 'm']})
        return df
    raise ValueError


def mocked_attribute_get_by_path(*args, **kwargs):
    class MockedAttribute:
        def __init__(self, web_id):
            self.web_id = web_id
    if args[0] == '\\\\assetserver_valid\\database_valid\\elements_valid|attributes_valid':
        return MockedAttribute(WEB_ID)
    raise ValueError

def mocked_stream_update_values_with_http_info(*args, **kwargs):
    if args[0] == WEB_ID:
        response = ({'items': None, 'links': None}, 202, {})
        return response
    raise ApiException

class PIInterfaceTest(unittest.TestCase):
    def setUp(self):
        host = "https://sample.com/piwebapi"
        user = 'user'
        password = 'password'
        self.client = PIInterface(host, user, password)

    # paramsで指定したパスが存在する場合
    @mock.patch(GET_PLOT_VALUES_PATH, side_effect=mocked_data_get_plot_values)
    def test_get_value_ok(self, mock_get):
        get_params = {
            'assetserver': 'assetserver_valid',
            'database': 'database_valid',
            'elements': ['elements_valid'],
            'attributes': ['attributes_valid']
        }
        df = self.client.get_values(**get_params)
        test_df = pd.DataFrame({'Value': [19, 24, 55, 12, 33]}, index=pd.date_range('20180401', '20180405'), dtype=np.int64)
        test_df.index.name = 'Timestamp'
        assert_frame_equal(df, test_df)

    # paramsで指定したパスが存在しない場合
    @mock.patch(GET_PLOT_VALUES_PATH, side_effect=mocked_data_get_plot_values)
    def test_get_value_ng(self, mock_get):
        with self.assertRaises(Exception):
            get_params = {
                'assetserver': 'assetserver_invalid',
                'database': 'database_valid',
                'elements': ['elements_valid'],
                'attributes': ['attributes_valid']
                }
            self.client.get_values(**get_params)

    # paramsで指定したパスが存在する場合
    @mock.patch(UPDATE_VALUES_PATH, side_effect=mocked_stream_update_values_with_http_info)
    @mock.patch(GET_PATH, side_effect=mocked_attribute_get_by_path)
    def test_update_value_ok(self, mock_post1, mock_post2):
        post_params = {
            'assetserver': 'assetserver_valid',
            'database': 'database_valid',
            'elements': ['elements_valid'],
            'attributes': ['attributes_valid']
        }
        post_df = pd.DataFrame(np.array([19, 24, 55, 12, 33]), columns=['attribute'], index=pd.date_range('20180401', '20180405'))
        response = self.client.update_values(post_df, **post_params)
        self.assertEqual(response[1], 202)

    # paramsで指定したパスが存在しない場合
    @mock.patch(UPDATE_VALUES_PATH, side_effect=mocked_stream_update_values_with_http_info)
    @mock.patch(GET_PATH, side_effect=mocked_attribute_get_by_path)
    def test_update_value_ng(self, mock_post1, mock_post2):
        with self.assertRaises(Exception):
            post_params = {
                'assetserver': 'assetserver_invalid',
                'database': 'database_valid',
                'elements': ['elements_valid'],
                'attributes': ['attributes_valid']
            }
            post_df = pd.DataFrame(np.array([19, 24, 55, 12, 33]), columns=['attribute'], index=pd.date_range('20180401', '20180405'))
            self.client.update_values(post_df, **post_params)

if __name__ == '__main__':
    unittest.main()
