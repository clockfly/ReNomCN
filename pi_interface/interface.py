import urllib3
import numpy as np
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models import PITimedValue
from functools import lru_cache

# This is needed to avoid authentication error for self-signed certificate of PI AF
urllib3.disable_warnings(InsecureRequestWarning)


class PIInterface(PIWebApiClient):
    """ Interface of PI AF to ReNom
    This class provides a interface of PI AF to ReNom
    
    Args:
        host (str): Host address.
        user (str): User name for basic authentification.
        password (str): Password for basic authentification.
    """
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        super().__init__(host, False, user, password, False)

    def get_values(self, assetserver, database, elements, attributes, start='*-7d', end='*', request_timeout=5):
        """This method returns values according to given path.
        Args:
            assetserver (str): Asset server name.
            database (str): Database name.
            elements(list): Name list for elements.
            attributes(list): Name list for attributes.
            start(str): Start time.
            end(str): End time.
            request_timeout(int): Timeout seconds.
        """
        path = 'af:\\\\%s\\%s\\%s|%s' % (assetserver, database, '\\'.join(elements), '|'.join(attributes))
        response = self._get_plot_values(path, None, start, None, None, end, None)
        df = response.set_index("Timestamp")[["Value"]]
        return df

    def _get_plot_values(self, path, desired_units, end_time, intervals, selected_fields, start_time, time_zone, request_timeout=5):
        if (path is None):
            print("The variable path cannot be null.")

        web_id = self._convert_path_to_web_id(path, request_timeout=request_timeout)
        res = super().data.streamApi.get_plot(web_id, desired_units, end_time, intervals, selected_fields, start_time,
                                      time_zone, _request_timeout=request_timeout)
        df = super().data.convert_to_df(res.items, selected_fields)
        return df

    @lru_cache(maxsize=None)
    def _convert_path_to_web_id(self, fullPath, request_timeout):
        system = fullPath[0:3]
        path = fullPath[3:None]
        if (system == "af:"):
            res = super().attribute.get_by_path(path, None, None, _request_timeout=request_timeout)
            return (res.web_id)
        elif (system == "pi:"):
            res = super().attribute.get_by_path(path, None, None, _request_timeout=request_timeout)
            return (res.web_id)
        else:
            print("Error: invalid path. It needs to start with \"pi\" or \"af\"")
            return

    def update_values(self, df, assetserver, database, elements, attributes, request_timeout=5):
        """This method saves DataFrame to PI AF.
        Args:
            df (pandas.DaraFrame): DataFrame of data to be saved.
            assetserver (str): Asset server name.
            database (str): Database name.
            elements(list): Name list for elements.
            attributes(list): Name list for attributes.
            request_timeout(int): Timeout seconds.
        """
        path = '\\\\%s\\%s\\%s|%s' % (assetserver, database, '\\'.join(elements), '|'.join(attributes))
        attribute = super().attribute.get_by_path(path, None, None, _request_timeout=request_timeout)
        item_list = []
        for i, s in df.iterrows():
            item = PITimedValue()
            item.timestamp = i
            # Dtype of updating DataFrame cannot be integer
            if issubclass(type(s.values[0]), np.integer):
                item.value = float(s.values[0])
            else:
                item.value = s.values[0]
            item_list.append(item)

        response = super().stream.update_values_with_http_info(attribute.web_id, item_list, None, None)
        return response
