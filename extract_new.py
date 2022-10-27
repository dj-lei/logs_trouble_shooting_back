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


class FileExtract(object):
    def __init__(self, path):
        self.express = ''
        self.regex = ['(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$', '\[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$']
        self.inverted_index_table = {}
        self.origin_lines = {}
        #     {
        #     'visby':{
        #         'radio': {
        #             'software':{
        #                 'main':[
        #                     ['(.*?): \[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$', ['device','timestamp','cost','name','trace','msg']],
        #                     ['\[(.*?)\] \((.*?)\) (.*?) (.*?): (.*?)$', ['timestamp','cost','name','trace','msg']]
        #                 ],
        #                 'msg_group':'process \= (.*?),.*?msg \= (.*)$'
        #             },
        #             'hardware':[],
        #             'elog': [],
        #         },
        #         'du':{
        #
        #         }
        #     }
        # }

        with open(path, 'r') as file:
            self.extract_inverted_index_table(file.readlines())

    def is_extractable_file(self, lines):
        judge_count = int(cf['COMMON']['FILE_JUDGE_COUNT'])
        for i, express in enumerate(self.regex):
            for count, line in enumerate(lines):
                if count < judge_count:
                    line = line.decode("utf-8") if type(line) == bytes else line
                    if len(re.findall(express[0], line)) > 0:
                        self.express = express
                        return True
                else:
                    break
        return False

    def extract_inverted_index_table(self, lines):
        for global_index, line in enumerate(lines):
            self.origin_lines[global_index] = line
            for word in set(clean_special_symbols(line.lower(), ' ').strip().split(' ')):
                if word not in self.inverted_index_table:
                    self.inverted_index_table[word] = {'global_index': [global_index]}
                else:
                    self.inverted_index_table[word]['global_index'].append(global_index)

    def search(self, cmd):
        leftP = []
        res = {}
        for i in range(0, len(cmd)):
            if cmd[i] == '(':
                leftP.append(i)

        for i in range(0, len(cmd)):
            if cmd[i] == '(':
                leftP.append(i)

            if cmd[i] == ')':
                priority = len(leftP)
                exp = cmd[leftP.pop(), i+1]
            if priority not in res:
                res[priority] = [exp]
            else:
                res[priority].append(exp)
        res[0] = [cmd]


