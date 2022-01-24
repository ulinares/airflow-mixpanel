import json
from pathlib import Path

from airflow.models.baseoperator import BaseOperator
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
        end_date: str = "{{next_da}}",
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
                f"Fetching records for {self.start_date} to {self.end_date}"
            )

            records = hook.get_data(
                start_date=self._start_date, end_date=self._end_date
            )
            self.log.info(f"Fetched {len(records)} records.")
        finally:
            hook.close()

        self.log.info(f"Writing records to {self._output_path}")

        output_path = Path(self._output_path)
        output_dir = output_path.parent.resolve()
        output_dir.mkdir(exist_ok=True, parents=True)

        with open(self._output_path, "w") as file_:
            json.dump(records, fp=file_)
