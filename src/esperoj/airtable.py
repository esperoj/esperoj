import os

from pyairtable import Api


class Airtable:
    def __init__(self):
        self.base_id = os.environ["AIRTABLE_BASE_ID"]
        self.api_key = os.environ["AIRTABLE_API_KEY"]
        self.client = Api(self.api_key)

    def table(self, table_name):
        return self.client.table(self.base_id, table_name)
