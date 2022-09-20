import re
import zlib
import base64
import datetime
import numpy as np
import pandas as pd


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
        for ch in ['/','*','{','}','[',']','(',')','#','+','-','!','=',':',',']:
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
                process = re.findall('process \= (.*?),', tmp)[0]
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

            kv = [(k.strip(), [v.strip()] if (v+' ')[0].isalpha() else re.findall('[0-9.|x]+', v)) for k, v in re.findall('([A-Za-z0-9_.]+?)[ ]?[:=][ ]?(.*?)[,$]', msg)]  # $ convenient regex at the end
            story.append([item['_source']['device'], item['_source']['trace'], process,  item['_source']['logtime'][:-1] + '.' + str(item['_source']['millisecond']) + 'Z', msg, kv])

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
                story_line[dev] = [{'process': process_name, 'start_time': process_start_time, 'start_count': process_start_count, 'end_time': process_end_time, 'end_count': process_end_count, 'msg': msg, 'kv': kv, 'k_type':k_type}]
            else:
                story_line[dev].append({'process': process_name, 'start_time': process_start_time, 'start_count': process_start_count, 'end_time': process_end_time, 'end_count': process_end_count, 'msg': msg, 'kv': kv, 'k_type':k_type})
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


############################################ XML Compression and Decompression ################################################
def decode_base64_and_inflate(b64string):
    decoded_data = base64.b64decode(b64string)
    return zlib.decompress(decoded_data , -15)


def deflate_and_base64_encode(string_val):
    zlibbed_str = zlib.compress(string_val)
    compressed_string = zlibbed_str[2:-4]
    return base64.b64encode(compressed_string).decode("utf-8")