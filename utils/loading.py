import os
import pandas as pd
import shutil
import tarfile

from pathlib import Path

UTILS_PATH = Path(__file__).parent.absolute()
MAIN_DIR_PATH = UTILS_PATH.parent
DATA_DIR_PATH = MAIN_DIR_PATH / 'data'
TAR_FILE_PATH = DATA_DIR_PATH / 'ES_fut_chain.tar'
EXTRACT_PATH = DATA_DIR_PATH / 'extracts'

ALL_DATA_FILES = []
with tarfile.open(TAR_FILE_PATH, 'r') as tar:
    for member in tar.getmembers():
        if member.isfile():
            ALL_DATA_FILES.append(member.name)

ALL_DATA_FILES.sort()


def read_data_from_tar(file_name,
                       tar_path=TAR_FILE_PATH,
                       extract_path=EXTRACT_PATH):
    with tarfile.open(tar_path, 'r') as tar:
        tar.extract(file_name, path=extract_path)
    df = pd.read_parquet(os.path.join(extract_path, file_name))
    return df


def read_multiple(num_files: int,
                  file_list: list = ALL_DATA_FILES,
                  tar_path: str = TAR_FILE_PATH,
                  extract_path: str = EXTRACT_PATH):
    data_list = [read_data_from_tar(file_name,
                                    tar_path=tar_path,
                                    extract_path=extract_path)
                 for file_name in file_list[:num_files]]
    return pd.concat(data_list)


def dir_empty(dir_path):
    return len(os.listdir(dir_path)) == 0


def clean_extracts(extract_path=EXTRACT_PATH):
    shutil.rmtree(extract_path)
