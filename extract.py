from utils import *
############################################ Data Clean ################################################
def package_kv(df):
    res = {}
    for i, (kv,index,timestamp) in enumerate(zip(df.kv.values, df['global_index'].values, df.timestamp.values)):
        for item in kv:
            if len(item[1]) > 0:
                if item[0] in res:
                    res[item[0]].append(item[1]  + [timestamp] + [str(i)] + [index]) # [value1,value2,value3,timestamp,process_index,global_index]
                else:
                    res[item[0]] = [item[1]  + [timestamp] + [str(i)] + [index]]
    for key in res.keys():
        width = max(map(len, res[key])) # get max width
        for i, item in enumerate(res[key]):         
            if len(item) != width:
                tmp = [0 for _ in range(0, width)]
                tmp[-3] = item[-3]
                tmp[-2] = item[-2]
                tmp[-1] = item[-1]
                res[key][i] = tmp
        res[key] = np.array(res[key]).transpose().tolist() # matrix transposition
    return res


def package_inverted_index_table(table, data, process):
    for index, msg in data:
        for word in set(clean_special_symbols(msg,' ').split(' ')):
            w = word.lower()
            if w not in table:
                table[w] = {'x': [str(index)], 'process': [process]} # x:global index, process: process name 
            else:
                table[w]['x'].append(str(index))
                table[w]['process'].append(process)


class FileExtract(object):
    def __init__(self, path, filename):
        self.filename = filename
        self.save_filename = []
        self.platform = ''
        self.device = ''
        self.express_num = 0
        self.is_extractable = False
        self.lines = []
        self.regex = {
            'visby':{
                'radio': {
                    'software':{
                        'main':[
                            ['(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$', ['device','timestamp','cost','name','trace','msg']],
                            ['\[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$', ['timestamp','cost','name','trace','msg']]
                        ],
                        'msg_group':'process \= (.*?),.*?msg \= (.*)$'
                    },
                    'hardware':[],
                    'elog': [],
                },
                'du':{

                }
            }
        }

        is_dcgm_zip_flag = False
        if '.zip' in path:
            is_dcgm_zip_flag, self.lines = self.is_dcgm_zip(path)
            # handle hwlog elog

        if is_dcgm_zip_flag == False:
            with open(path, 'r') as file:
                self.lines = file.readlines()

        if self.is_extractable_file(self.lines) == True:
            self.is_extractable = True
        else:
            self.is_extractable = False

    def is_dcgm_zip(self, path):
        path_logfiles = ''
        teread = 'teread.log'
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
                        try:
                            with nested_zip.open(teread, 'r') as file:
                                return True, file.readlines()
                        except:
                            return False, ''

    def is_extractable_file(self, lines):
        judge_count = int(cf['COMMON']['FILE_JUDGE_COUNT'])
        for platform in self.regex.keys():
            for device in self.regex[platform].keys():
                for i, express in enumerate(self.regex[platform][device]['software']['main']):
                    for count, line in enumerate(lines):
                        if count < judge_count:
                            line = line.decode("utf-8") if type(line) == bytes else line
                            if len(re.findall(express[0], line)) > 0:
                                self.platform = platform
                                self.device = device
                                self.express_num = i
                                return True
                        else:
                            break
        return False

    def extract_kv_and_inverted_index_table(self, data):
        def clean_msg_special_symbols(text):
            for ch in ['{', '}', '[', ']', '(', ')', '"', '::', '\'']:
                if ch in text:
                    text = text.replace(ch, " ")
            return re.sub(" +", " ", text)

        msg_group_regex = self.regex[self.platform][self.device]['software']['msg_group']
        extracted_data = []
        for item in data:
            tmp = clean_msg_special_symbols(item['msg'])
            ret = re.findall(msg_group_regex, tmp)
            if len(ret) > 0:
                process = ret[0][0].strip()
                msg = ret[0][1].strip()
            else:
                process = 'main'
                msg = tmp.strip()

            # extract key value
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
            item['process'] = process
            item['kv'] = kv
            extracted_data.append(item)

        extracted_data = pd.DataFrame(extracted_data).sort_values('timestamp',ascending=True).reset_index(drop=True)

        # package kvs msgs and inverted_index_table
        inverted_index_table = {}
        kvs = {}
        msgs = {}
        for process_name in sorted(set(extracted_data.process.values)):
            process = extracted_data.loc[(extracted_data['process'] == process_name), :].reset_index()
            process['global_index'] = process['index']
            package_inverted_index_table(inverted_index_table, zip(process['global_index'].values, process.msg.values), process_name)
            kvs[process_name] = package_kv(process)

            tmp = {}
            for global_index, timestamp, msg in process[['global_index','timestamp','msg']].values:
                tmp[global_index] = {'timestamp':timestamp, 'msg': msg}
            msgs[process_name] = tmp

        # sorted inverted_index_table key
        # for key in inverted_index_table.keys():
        #     inverted_index_table[key] = sorted(inverted_index_table[key])

        return {'origin_logs':msgs, 'kv': kvs, 'inverted_index_table': inverted_index_table}

    def extract(self):
        logs = {}
        express = self.regex[self.platform][self.device]['software']['main'][self.express_num][0]
        columns = self.regex[self.platform][self.device]['software']['main'][self.express_num][1]
        for line in self.lines:
            line = line.decode("utf-8") if type(line) == bytes else line
            ret = re.findall(express, line)
            if len(ret) > 0:
                v = dict(zip(columns, ret[0]))
                if 'device' not in v:
                    v['device'] = '_' + v['name']
                else:
                    v['device'] = v['device'] + '_' + v['name']
                
                if v['device'] not in logs:
                    logs[v['device']] = []
                logs[v['device']].append(v)

        res = []
        for device in logs.keys():
            for dev in cf['COMMON']['MATCH_DEVICE'].split(','):
                if dev in device:
                    res = self.extract_kv_and_inverted_index_table(logs[device])
                    self.save(cf['ENV_'+env]['LOG_STORE_PATH']+self.filename+'_'+device+'_'+datetime.datetime.now().strftime("%Y_%m_%d"), res)
                    self.save_filename.append(self.filename+'_'+device+'_'+datetime.datetime.now().strftime("%Y_%m_%d"))

    def save(self, path, data):
        with open(path, 'wb') as save_file:
            save_file.write(gzip_compress(data))
            # save_file.write(deflate_and_base64_encode(json.dumps(data).encode('utf-8')))
            # save_file.write(json.dumps(data))

