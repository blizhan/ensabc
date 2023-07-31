import requests
import json

import boto3
import botocore
import pandas as pd
import numpy as np

def parse_ecmwf_index_detail(index_url: str) -> pd.DataFrame:
    """get the detail messages of the grib2 file of the given url

    Args:
        index_url (str): grib file url

    Returns:
        pd.DataFrame: mesaages details
    """

    res_dict = []
    try:
        r = requests.get(index_url)
        content = r.text
        for c in content.split("\n")[:]:
            if len(c) == 0:
                continue
            d = {}
            for k, v in json.loads(c).items():
                if k == "_offset":
                    d["start"] = str(v)
                if k == "_length":
                    d["end"] = str(int(d["start"]) + int(v))
                if k == "levelist":
                    if v is not None:
                        d[k] = int(v)
                    else:
                        d[k] = np.nan
                else:
                    d[str(k)] = str(v)
            if d.get("levelist") is None:
                if d["param"] == "2t":
                    d["levelist"] = 2
                elif d["param"] in ["10u", "10v"]:
                    d["levelist"] = 10
                else:
                    d["levelist"] = 0
            res_dict.append(d)
    except Exception as e:
        pass
    df = pd.DataFrame(res_dict[:])
    return df


def parse_gfs_index_detail(index_url: str) -> pd.DataFrame:
    """get the detail messages of the grib2 file of the given url

    Args:
        index_url (str): grib file url

    Returns:
        pd.DataFrame: mesaages details
    """

    res_dict = []
    try:
        client = boto3.client(
            "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
        )
        bucket = index_url.split("/")[0]
        prefix = "/".join(index_url.split("/")[1:])
        r = client.get_object(Bucket=bucket, Key=prefix)
        content = r.get("Body").read().decode('utf-8')

        # r = requests.get(index_url)
        # content = r.text
        for c in content.split("\n")[:]:
            if len(c) == 0:
                continue
            d = {}
            datas = c.split(':')
            d['message_index'] = int(datas[0])
            d['start'] = int(datas[1])
            d['date'] = datas[2].split('d=')[1][:8]
            d['time'] = datas[2].split('d=')[1][8:10]+'00'
            d['shortName'] = datas[3]
            d['level'] = datas[4]
            d['step'] = datas[5]
            d['type'] = datas[6]
            res_dict.append(d)
    except Exception as e:
        pass

    df = pd.DataFrame(res_dict).sort_values('message_index')
    df['end'] = df['start'].shift(-1)
    df['end'] = df['end'].fillna(-1).astype(int)
    return df

def groupby_offset_groups(df: pd.DataFrame) -> pd.DataFrame:
    """groupby offset (join continuous messages)

    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    df['start'] = df['start'].astype(int)
    df['end'] = df['end'].astype(int)
    # df['_length'] = df['_length'].astype(int)

    df = df.sort_values('start')
    df['group'] = np.nan
    df = df.reset_index().copy()

    start = None
    end = None
    group = 0
    
    for i in df.iterrows():
        data = i[1]
        if start is None and end is None:
            start = int(data['start'])
            end = int(data['end'])
        elif end == int(data['start']):
            end = int(data['end'])
        elif end != int(data['start']):
            start = int(data['start'])
            end = int(data['end'])
            df['group'].iloc[int(i[0]-1)]=group

            group += 1
        if i[0]+1 == len(df):
            if end == int(data['start']):
                df['group'].iloc[i[0]]=group+1
            elif end != int(data['start']):
                df['group'].iloc[i[0]]=group
    
    df['group'] = df['group'].bfill().astype(int)

    return df.groupby('group').agg({'start': 'min', 'end':'max'})
