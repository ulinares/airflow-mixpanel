import csv
import json
from io import BytesIO
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults

from airflow_mixpanel.hooks import MixpanelHook


class MixpanelFetchRecordsOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        output_path: str,
        start_date: str = "{{ds}}",
        end_date: str = "{{next_ds}}",
        **kwargs,
    ):
        super(MixpanelFetchRecordsOperator, self).__init__(**kwargs)
        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date
        self._end_date = end_date

    def execute(self, context):
        hook = MixpanelHook(self._conn_id)

        try:
            self.log.info(
                f"Fetching records for {self._start_date} to {self._end_date}"
            )

            records = hook.get_data(
                start_date=self._start_date, end_date=self._end_date
            )
            self.log.info(f"Fetched {len(records)} records.")
        except:
            self.log.error("An error ocurred while fetching the records.")

        self.log.info(f"Writing records to {self._output_path}")

        output_path = Path(self._output_path)
        output_dir = output_path.parent.resolve()
        output_dir.mkdir(exist_ok=True, parents=True)

        with open(self._output_path, "w") as file_:
            json.dump(records, fp=file_)


class MixpaneltoS3Operator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_dest_bucket_key")

    @apply_defaults
    def __init__(
        self,
        mixpanel_conn_id: str,
        s3_conn_id: str,
        start_date: str,
        end_date: str,
        dest_bucket_name: str,
        dest_bucket_key: str,
        output_key_delimiter: str = "|",
        **kwargs,
    ):
        super(MixpaneltoS3Operator, self).__init__(**kwargs)
        self._mixpanel_conn_id = mixpanel_conn_id
        self._s3_conn_id = s3_conn_id
        self._start_date = start_date
        self._end_date = end_date
        self._dest_bucket_name = dest_bucket_name
        self._dest_bucket_key = dest_bucket_key
        self._output_key_delimiter = output_key_delimiter

    def execute(self, context):
        mixpanel_hook = MixpanelHook(self._mixpanel_conn_id)
        s3_hook = S3Hook(self._s3_conn_id)

        try:
            self.log.info(
                f"Fetching records for {self._start_date} to {self._end_date}."
            )
            records = mixpanel_hook.get_data(
                start_date=self._start_date, end_date=self._end_date
            )
            self.log.info(f"Fetched {len(records)} records.")

            self.log.info(
                f"Uploading records to {self._dest_bucket_name} bucket."
            )

            # bytes_stream = BytesIO()
            json_data = json.dumps(records)
            binary_data = json_data.encode()
            bytes_stream = BytesIO(binary_data)

            bytes_stream.seek(0)
            s3_hook.load_file_obj(
                bytes_stream,
                key=self._dest_bucket_key,
                bucket_name=self._dest_bucket_name,
                replace=True,
            )
        except:
            self.log.exception("An error ocurred.")
