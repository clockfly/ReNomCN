import urllib3
import numpy as np
import pandas as pd
from urllib3.exceptions import InsecureRequestWarning
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models import PITimedValue

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

    def get_values(self, assetserver, database, elements, attributes, start='*-7d', end='*'):
        """This method returns values according to given path.
        Args:
            assetserver (str): Asset server name.
            database (str): Database name.
            elements(list): Name list for elements.
            attributes(list): Name list for attributes.
            start(str): Start time.
            end(str): End time.
        """
        path = 'af:\\\\%s\\%s\\%s|%s' % (assetserver, database, '\\'.join(elements), '|'.join(attributes))
        response = super().data.get_plot_values(path, None, start, None, None, end, None)

        df = response.set_index("Timestamp")[["Value"]]
        return df

    def update_values(self, df, assetserver, database, elements, attributes):
        """This method saves DataFrame to PI AF.
        Args:
            df (pandas.DaraFrame): DataFrame of data to be saved.
            assetserver (str): Asset server name.
            database (str): Database name.
            elements(list): Name list for elements.
            attributes(list): Name list for attributes.
        """
        path = '\\\\%s\\%s\\%s|%s' % (assetserver, database, '\\'.join(elements), '|'.join(attributes))
        attribute = super().attribute.get_by_path(path, None, None)
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
