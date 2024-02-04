from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
import pyarrow as pa

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./magic-zoomcamp/keys/terraform_service.json"
bucket_name = 'ny_gr_taxi_data_bucket'
project_id = 'spry-abacus-411519'
table_name = 'nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(data, *args, **kwargs) -> None:
    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()
    pa.parquet.write_to_dataset(
        table,
        root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
