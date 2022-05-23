import glob

from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeHook


def local_to_adls(dir_target, ds, filepath):
    adls = AzureDataLakeHook(wasb_conn_id='conn_data_lake_raw')
    if glob.glob(filepath):
        for f in glob.glob(filepath):
            print("File to move {}".format(f))
            blob_path = dir_target + ds + '/' + f.split('/')[-1]
            adls.upload_file(local_path=f, remote_path=blob_path)
    else:
        raise ValueError('Directory is empty no file to copy')


