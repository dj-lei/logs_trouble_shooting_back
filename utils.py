import re
import io
import os
import time
import json
import zlib
import gzip
import uuid
import base64
import zipfile
import datetime
import numpy as np
import pandas as pd
import configparser
from sys import platform

cf = configparser.ConfigParser()
cf.read('config/config.cfg')

env = 'DEVELOP'
if 'win' in platform:
    env = 'DEVELOP'
elif 'linux' in platform:
    env = 'PRODUCT'


def clean_special_symbols(text, symbol):
    for ch in ['/','*','{','}','[',']','(',')','#','+','-','!','=',':',',','"','\'','>','<','@','`','$','%','^','&','|']:
        if ch in text:
            text = text.replace(ch,symbol)
    return re.sub(symbol+"+", symbol, text)


def iterate_files_in_directory(directory):
    # iterate over files in
    # that directory
    res = []
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            res.append(filename)
    return res

def cal_time_difference(start, end):
    return datetime.datetime.strptime(end, "%H:%M:%S") - datetime.datetime.strptime(start, "%H:%M:%S")


############################################ Json Compression and Decompression ################################################
def decode_base64_and_inflate(string, isB64decode=False):
    if isB64decode:
        decoded_data = base64.b64decode(string)
    else:
        decoded_data = string
    return zlib.decompress(decoded_data , -15)


def deflate_and_base64_encode(string_val, isB64encode=False):
    zlibbed_str = zlib.compress(string_val)
    compressed_string = zlibbed_str[2:-4]
    if isB64encode:
        return base64.b64encode(compressed_string).decode("utf-8")
    else:
        return compressed_string

def gzip_compress(data):
    return gzip.compress(json.dumps(data).encode('utf8'), 5)

def gzip_decompress(data):
    return json.loads(gzip.decompress(data))