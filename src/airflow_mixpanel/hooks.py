import json
from typing import List

import requests
from airflow.hooks.base_hook import BaseHook


class MixpanelHook(BaseHook):
    DEFAULT_HOST = "data.mixpanel.com/api/2.0/export"
    DEFAULT_SCHEMA = "https"

    def __init__(self, conn_id: str):
        super().__init__()
        self._conn_id = conn_id
        self._session = None
        self._url = None

    def get_conn(self):
        """
        Returns the connection for Mixpanel to be used by the hook.
        """

        if self._session is None:
            session = requests.Session()

            config = self.get_connection(self._conn_id)
            schema = config.schema or self.DEFAULT_SCHEMA
            host = config.host or self.DEFAULT_HOST
            url = f"{schema}://{host}"

            if config.password:
                session.auth = (config.password, None)
            else:
                raise Exception("Missing password.")

            self._session = session
            self._url = url

        return self._session, self._url

    def get_data(self, start_date: str, end_date: str) -> List[dict]:
        """
        Get Mixpanel data for the specified date range (start_date, end_date).

        Parameters
        -----
        start_date: str
        Start date to start fetching the records. The expected format is YYYY-MM-DD.
        end_date: str
        End date to fetching records up to (inclusive). The expected format is YYYY-MM-DD.
        """
        session, url = self.get_conn()
        params = {"from_date": start_date, "to_date": end_date}
        headers = {"Accept": "text/plain"}
        resp = session.get(url, headers=headers, params=params)
        resp.raise_for_status()
        content = resp.content.decode("utf8").split("\n")
        content.pop()
        json_content = [json.loads(line) for line in content]
        json_content = [
            {"event": record["event"], **record["properties"]}
            for record in json_content
        ]

        return json_content
