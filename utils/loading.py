import os
import pandas as pd
import shutil
import tarfile


def read_data_from_tar(file_name, tar_path, extract_path):
    with tarfile.open(tar_path, 'r') as tar:
        tar.extract(file_name, path=extract_path)

    df = pd.read_parquet(os.path.join(extract_path, file_name))

    return df


# def read_multiple(num_files, tar_path, extract_path):
#     data_list = [read_data_from_tar(file_name,
#                                     tar_path=tar_path,
#                                     extract_path=extract_path)
#                  for file_name in ALL_DATA_FILES[:num_files]]

#     return pd.concat(data_list)


def dir_empty(dir_path):
    return len(os.listdir(dir_path)) == 0


def clean_extracts(extract_path):
    shutil.rmtree(extract_path)
