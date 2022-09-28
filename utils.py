import re
import io
import zlib
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


############################################ Data Clean ################################################
def package_kv(df):
    res = {}
    k_type = {}
    for i, (kv,index,timestamp) in enumerate(zip(df.kv.values, df['index'].values, df.timestamp.values)):
        for item in kv:
            if len(item[1]) > 0:
                if item[0] in res:
                    res[item[0]].append(item[1]  + [timestamp] + [str(i)] + [index]) # [value1,value2,value3,timestamp,process_index,global_index]
                else:
                    res[item[0]] = [item[1]  + [timestamp] + [str(i)] + [index]]
    for key in res.keys():
        width = max(map(len, res[key])) # get max width
        type_list = []
        for i, item in enumerate(res[key]):
            if '0x' in item[0]:
                type_list.append('register')
            elif item[0].isupper():
                type_list.append('discrete')
            else:
                type_list.append('continuous')
                
            if len(item) != width:
                tmp = [0 for _ in range(0, width)]
                tmp[-3] = item[-3]
                tmp[-2] = item[-2]
                tmp[-1] = item[-1]
                res[key][i] = tmp
        res[key] = np.array(res[key]).transpose().tolist() # matrix transposition
        k_type[key] = 'discrete' if len(set(type_list)) > 1 else list(set(type_list))[0]
    return res,k_type


def package_inverted_index_table(table, key, data):
    def clean_special_symbols(text):
        for ch in ['/','*','{','}','[',']','(',')','#','+','-','!','=',':',',','"']:
            if ch in text:
                text = text.replace(ch," ")
        return re.sub(" +", " ", text)
    for index, msg in data:
        for word in set(clean_special_symbols(msg).split(' ')):
            w = word.lower()
            if w not in table:
                table[w] = {'x': [index], 'y': [key]} # x:global index, y: yaxis num 
            else:
                table[w]['x'].append(index)
                table[w]['y'].append(key)


def clean_data(esdata):
    def clean_msg_special_symbols(text):
        for ch in ['{', '}', '[', ']', '(', ')', '"', '::']:
            if ch in text:
                text = text.replace(ch, " ")
        return re.sub(" +", " ", text)

    story = []
    for item in esdata:
        if 'msg' in item['_source']:
            tmp = clean_msg_special_symbols(item['_source']['msg'])
            if len(re.findall('process \= (.*?)$', tmp)) > 0:
                process = re.findall('process \= (.*?),', tmp)[0].strip()
                msg = re.findall('msg \= (.*?)$', tmp)[0]
            #                 fileAndLine = re.findall('fileAndLine \= \"(.*?)\"',item['_source']['msg'])[0].split(':')[0]
            # elif len(re.findall('procname \= (.*?)$', tmp)) > 0:
            #     process = re.findall('procname \= (.*?),', tmp)[0]
            #     #                 msg = tmp.split(',')[2].replace('"','').replace('}','').replace('{','')
            #     msg = tmp
            else:
                process = 'main'
                msg = tmp

            msg = msg.replace('= ',':').replace(' = ',':').replace(': ',':').replace(' : ',':').replace('=',':')

            for elm in re.split('[: ]',msg):
                if elm.isupper():
                    msg = re.sub('[: ]'+elm, ':'+elm, msg)

            msg = re.sub('(:(?!-).*?[ $])', r'\1,', (msg + ' $'))
            kv = []
            for k, v in re.findall('([A-Za-z0-9_.]+?)[ ]?[:=][ ]?(.*?)[,$]', msg):
                if len(v.strip()) > 0:
                    if (v.strip()+'xx').lower()[0:2] == '0x':
                        kv.append((k.strip()+'(r)',  [v.strip()]))
                    elif v.strip()[0].isalpha():
                        kv.append((k.strip()+'(d)', [v.strip()]))
                    else:
                        kv.append((k.strip()+'(c)', re.findall('[0-9.]+', v)))  # $ convenient regex at the end
            story.append([item['_source']['device'], item['_source']['trace'], process,  item['_source']['logtime'][:-1] + '.' + str(item['_source']['millisecond']) + 'Z', item['_source']['msg'], kv])

    story = pd.DataFrame(story, columns=['device', 'trace', 'process', 'timestamp', 'msg', 'kv']).sort_values('timestamp',ascending=True).reset_index(drop=True)
    story_line = {}
    inverted_index_table = {}
    for dev in set(story.device.values):
        data = story.loc[(story['device'] == dev), :].reset_index(drop=True)
        sub_inverted_index_table = {}
        for i, process_name in enumerate(sorted(set(data.process.values), key=list(data.process.values).index)):
            process = data.loc[(data['process'] == process_name), :].reset_index()
            process['index'] = process['index'].astype(str)
            process_start_time = process['timestamp'][0]
            process_start_count = process['index'][0]
            process_end_time = process['timestamp'][process.shape[0] - 1]
            process_end_count = process['index'][process.shape[0] - 1]
            package_inverted_index_table(sub_inverted_index_table, i, zip(process['index'].values, process.msg.values))
            msg = dict(zip(process['index'].values, [str(a) + '||' + b + '||' + c for a, b, c in
                                                     zip(process.index.values, process.timestamp.values,
                                                         process.msg.values)]))
            kv, k_type = package_kv(process)
            if dev not in story_line:
                story_line[dev] = [{'process': process_name, 'start_time': process_start_time, 'start_count': process_start_count, 'end_time': process_end_time, 'end_count': process_end_count, 'msg': msg, 'kv': kv}]
            else:
                story_line[dev].append({'process': process_name, 'start_time': process_start_time, 'start_count': process_start_count, 'end_time': process_end_time, 'end_count': process_end_count, 'msg': msg, 'kv': kv})
        inverted_index_table[dev] = sub_inverted_index_table
    return {'story_line': story_line, 'inverted_index_table': inverted_index_table}


def apply_filter_by_keywords(df):
    if (len(set(df['msg']) & set([':', '='])) > 0):
        return False
    return True


def apply_filter_digit(df):
    return re.sub('\d+', '', df['msg'])


def apply_keyword_highlight(df, keywords, color_highlight):
    tmp = [item.lower() for item in keywords]
    for item in tmp:
        if (item == 'abn:') & (item in df['msg'].lower()):
            return color_highlight
        elif len(set(df['msg'].lower().split(' ')).intersection(set(tmp))) > 0:
            return color_highlight
    return df['status']


def cal_time_difference(start, end):
    return datetime.datetime.strptime(end, "%H:%M:%S") - datetime.datetime.strptime(start, "%H:%M:%S")

############################################ judge file format and save ################################################
def is_dcgm_zip(path, file_name):
#     path = 'GLT_SUKAMULYA_CBN_CM_220715_064452_WIB_MSRBS-GL_CXP9024418-15_R24M11_dcgm.zip'
#     path_logfiles = 'GLT_SUKAMULYA_CBN_CM_logfiles.zip'
    path_logfiles = ''
    teread = 'teread.log'
    regex = '(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$'
    judge_count = 50
    logs = {}
    dev = ''
    log_flag = False
    is_extrac = False
    with zipfile.ZipFile(path, 'r') as outer:
        for file in outer.filelist:
            if 'logfiles.zip' in file.filename:
                path_logfiles = file.filename
                break
        if path_logfiles == '':
            return False,''
        else:
            with outer.open(path_logfiles, 'r') as nest:
                logfiles = io.BytesIO(nest.read())
                with zipfile.ZipFile(logfiles) as nested_zip:
                    with nested_zip.open(teread, 'r') as log:
                        lines = log.readlines()
                        count = 0
                        for i, line in enumerate(lines):
                            line = line.decode("utf-8")
                            if len(line) > 0:
                                if line[0] == '=':
                                    log_flag = False
                                    is_extrac = False
                                    count = 0
                            if log_flag:
                                logs[dev].append(line[0:-1])
                                if count < judge_count:
                                    if (is_extrac == False):
                                        count = count + 1
                                        if len(re.findall(regex, line)) > 0:
                                            is_extrac = True
                                else:
                                    if (is_extrac == False):
                                        logs.pop(dev, None)
                                        log_flag = False

                            if ('coli>' in line) & ('te log read' in line):
                                dev = line.split(' ')[1]
                                logs[dev] = []
                                log_flag = True
    files = []
    if len(logs.keys()) == 0:
        return False, files
    else:
        for key in logs.keys():
            index_name = file_name.split('.zip')[0] +'_'+key+'_telog_'+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            index_name = index_name.lower()
            store_path = cf['ENV_'+env]['LOG_STORE_PATH'] + index_name +'.log'
            files.append(index_name)
            with open(store_path, 'w') as fp:
                fp.write("\n".join(item for item in logs[key]))
        return True, files

def is_multi_devices_telog_log(path, file_name):
    res = []
    regex = '(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$'
    judge_count = 50
    logs = {}
    dev = ''
    log_flag = False
    is_extrac = False
    with open(path, 'r') as log:
        lines = log.readlines()
        count = 0
        for i, line in enumerate(lines):
            if len(line) > 0:
                if line[0] == '=':
                    log_flag = False
                    is_extrac = False
                    count = 0
            if log_flag:
                logs[dev].append(line[0:-1])
                if count < judge_count:
                    if (is_extrac == False):
                        count = count + 1
                        if len(re.findall(regex, line)) > 0:
                            is_extrac = True
                else:
                    if (is_extrac == False):
                        logs.pop(dev, None)
                        log_flag = False

            if ('coli>' in line) & ('te log read' in line):
                dev = line.split(' ')[1]
                logs[dev] = []
                log_flag = True

    files = []
    if len(logs.keys()) == 0:
        return False, files
    else:
        for key in logs.keys():
            index_name = file_name +'_'+key+'_telog_'+datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
            index_name = index_name.lower()
            store_path = cf['ENV_'+env]['LOG_STORE_PATH'] + index_name +'.log'
            files.append(index_name)
            with open(store_path, 'w') as fp:
                fp.write("\n".join(item for item in logs[key]))
    return True, files

def is_single_device_telog_log(path, file_name):
    res = []
    regex = '(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$'
    judge_count = 50
    is_extrac = False
    with open(path, 'r') as log:
        lines = log.readlines()
        count = 0
        for i, line in enumerate(lines):
            line = line
            res.append(line[0:-1])
            if count < judge_count:
                if (is_extrac == False):
                    count = count + 1
                    if len(re.findall(regex, line)) > 0:
                        is_extrac = True
            else:
                if (is_extrac == False):
                    return False, ''
    
    index_name = file_name + '_telog_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    index_name = index_name.lower()
    store_path = cf['ENV_'+env]['LOG_STORE_PATH'] + index_name +'.log'
    with open(store_path, 'w') as fp:
        fp.write("\n".join(res))
    return True, index_name

def is_lab_log(path, file_name):
    res = []
    regex = '\[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$'
    judge_count = 50
    is_extrac = False
    with open(path, 'r') as log:
        lines = log.readlines()
        count = 0
        for i, line in enumerate(lines):
            line = line
            res.append(line[0:-1])
            if count < judge_count:
                if (is_extrac == False):
                    count = count + 1
                    if len(re.findall(regex, line)) > 0:
                        is_extrac = True
            else:
                if (is_extrac == False):
                    return False, ''
    index_name = file_name + '_lab_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    index_name = index_name.lower()
    store_path = cf['ENV_'+env]['LOG_STORE_PATH'] + index_name +'.log'
    with open(store_path, 'w') as fp:
        fp.write("\n".join(res))
    return True, index_name

############################################ XML Compression and Decompression ################################################
def decode_base64_and_inflate(b64string):
    decoded_data = base64.b64decode(b64string)
    return zlib.decompress(decoded_data , -15)


def deflate_and_base64_encode(string_val):
    zlibbed_str = zlib.compress(string_val)
    compressed_string = zlibbed_str[2:-4]
    return base64.b64encode(compressed_string).decode("utf-8")