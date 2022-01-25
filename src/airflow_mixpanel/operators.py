import csv
from io import BytesIO, StringIO
from pathlib import Path
from typing import List

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
        file_delimiter: str = "|",
        **kwargs,
    ):
        super(MixpanelFetchRecordsOperator, self).__init__(**kwargs)
        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date
        self._end_date = end_date
        self._file_delimiter = file_delimiter

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
            header_names = set(
                [key for record in records for key in record.keys()]
            )
            writer = csv.DictWriter(
                file_, header_names, delimiter=self._file_delimiter
            )
            writer.writeheader()
            writer.writerows(records)


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

    def _records_to_buffer(
        self, records_list: List[dict], delimiter: str = "|"
    ):
        string_buffer = StringIO()

        header_names = set(
            [key for record in records_list for key in record.keys()]
        )
        writer = csv.DictWriter(
            string_buffer, header_names, delimiter=delimiter
        )
        writer.writeheader()
        writer.writerows(records_list)

        bytes_buffer = BytesIO(string_buffer.getvalue().encode())
        string_buffer.close()
        bytes_buffer.seek(0)

        return bytes_buffer

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

            records_bytes_buffer = self._records_to_buffer(
                records, delimiter=self._output_key_delimiter
            )

            s3_hook.load_file_obj(
                records_bytes_buffer,
                key=self._dest_bucket_key,
                bucket_name=self._dest_bucket_name,
                replace=True,
            )
        except:
            self.log.exception("An error ocurred.")
