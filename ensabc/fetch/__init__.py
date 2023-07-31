import os
import shutil
import subprocess
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import boto3
import botocore
import pandas as pd

from ..parse import groupby_offset_groups

PARALLEL_NUM = 3

def single_range_download(
    download_url: str,
    start_bytes: str,
    end_bytes: str,
    local_fp: str,
) -> int:
    """download bytes range of single file from download url to local path using curl command

    Parameters:
        download_url: str, download url
        start_bytes: str, the download_range bytes start
        end_bytes: str, the download_range bytes end
        local_fp: str, local filepath
    return:
        int, the bytes size of file
    """
    tmp = local_fp + ".tmp"
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)

    dir = os.path.dirname(tmp)
    if len(dir):
        os.makedirs(dir, exist_ok=True)
    try:
        cmd = f"curl -s --range {start_bytes}-{end_bytes} {download_url} > {tmp}"
        os.system(cmd)
    except Exception as e:
        os.remove(tmp)
        raise Exception from e

    shutil.move(tmp, local_fp)
    return os.path.getsize(local_fp)

def single_session_download(
    download_url: str, 
    local_fp: str
) -> int:
    """download single file from download url to local path using session, and verify

    Parameters:
        download_url: str, download url
        local_fp: str, local filepath
    
    return:
        int, the bytes size of file
    """
    session = None
    tmp = local_fp + ".tmp"
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)

    try:
        session = requests.Session()
        resp = session.get(download_url, stream=True, timeout=60 * 5)
        with open(tmp, "wb") as f:
            f.write(resp.content)
            f.flush()
        session.close()
    except Exception as e:
        os.remove(tmp)
        session.close()
        raise Exception from e

    shutil.move(tmp, local_fp)
    return os.path.getsize(local_fp)

def s3_single_range_download(
    download_url: str,
    start_bytes: str,
    end_bytes: str,
    local_fp: str,
) -> int:
    """download bytes range of single file from download url to local path using curl command

    Parameters:
        download_url: str, download url
        start_bytes: str, the download_range bytes start
        end_bytes: str, the download_range bytes end
        local_fp: str, local filepath
    return:
        int, the bytes size of file
    """
    tmp = local_fp + ".tmp"
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)

    dir = os.path.dirname(tmp)
    if len(dir):
        os.makedirs(dir, exist_ok=True)
    try:
        client = boto3.client(
            "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
        )
        bucket = download_url.split("/")[0]
        prefix = "/".join(download_url.split("/")[1:])
        if end_bytes is not None:
            r = client.get_object(Bucket=bucket, Key=prefix, Range=f'bytes={start_bytes}-{end_bytes}')
        else:
            r = client.get_object(Bucket=bucket, Key=prefix)

        with open(tmp, 'wb') as f:
            for data in iter(lambda: r['Body'].read(100 * 1024), b""):
                f.write(data)
                f.flush()
        # cmd = f"curl -s --range {start_bytes}-{end_bytes} {download_url} > {tmp}"
        # os.system(cmd)
    except Exception as e:
        os.remove(tmp)
        raise Exception from e

    shutil.move(tmp, local_fp)
    return os.path.getsize(local_fp)

def batch_range_download(
    inputs_list: list, thread_num: int = 5
):
    """range-download multi files from download urls to local path using curl, and handle logger

    Parameters:
        inputs_list: list, [(url, start_bytes, end_bytes, local filepath),...]
        thread_num: int, default is 5
    return:
        fail: list
    """
    futures = []
    fail = []
    with ThreadPoolExecutor(thread_num) as pool:
        for i in inputs_list:
            futures.append(
                pool.submit(
                    single_range_download,
                    download_url=i[0],
                    start_bytes=i[1],
                    end_bytes=i[2],
                    local_fp=i[3],
                )
            )
        for n, f in enumerate(as_completed(futures)):
            # f.result()
            try:
                f.result()
            except Exception as e:
                fail.append(inputs_list[n])
    return fail

def s3_batch_range_download(
    inputs_list: list, thread_num: int = 5
):
    """range-download multi files from download urls to local path using curl, and handle logger

    Parameters:
        inputs_list: list, [(url, start_bytes, end_bytes, local filepath),...]
        thread_num: int, default is 5
    return:
        fail: list
    """
    futures = []
    fail = []
    with ThreadPoolExecutor(thread_num) as pool:
        for i in inputs_list:
            futures.append(
                pool.submit(
                    s3_single_range_download,
                    download_url=i[0],
                    start_bytes=i[1],
                    end_bytes=i[2],
                    local_fp=i[3],
                )
            )
        for n, f in enumerate(as_completed(futures)):
            # f.result()
            try:
                f.result()
            except Exception as e:
                fail.append(inputs_list[n])
    return fail

def grib_detail_download(
    download_url: str,
    local_fp: str,
    grib_detail: pd.DataFrame=None,
    typing: str='url'
) -> str:
    
    dir = os.path.dirname(local_fp)
    if len(dir):
        os.makedirs(dir, exist_ok=True)

    if isinstance(grib_detail, type(None)):
        if typing == 'url':
            single_session_download(download_url, local_fp)
        elif typing == 's3':
            s3_single_range_download(download_url, None, None, local_fp)
    else:
        if len(grib_detail)>1:
            groups = groupby_offset_groups(grib_detail)
        else:
            grib_detail.loc[:,('group')]=0
            groups = grib_detail.groupby('group').agg({'start': 'min', 'end':'max'})
        inputs_list = [
            (download_url, i[1]['start'], i[1]['end'], f'{local_fp}.tmp{i[0]}') for i in groups.iterrows()
        ]
        if typing == 'url':
            batch_range_download(inputs_list)
        elif typing == 's3':
            s3_batch_range_download(inputs_list)
        files = [i[3] for i in inputs_list]

        merge_grib(files, local_fp)
    
    return local_fp


def merge_grib(files:list, outfile:str):
    """merge mulit grib files as one grib

    Args:
        files (list): grib files
        outfile (str): out file

    Returns:
        _type_: _description_
    """
    merge_cmd = f"grib_copy {' '.join(files)} {outfile}"
    subprocess.check_call(merge_cmd, shell=True)
    for f in files:
        os.remove(f)