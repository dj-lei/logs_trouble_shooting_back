import atexit
import warnings
import configparser
from utils import *
from sys import platform
from drawio import Drawio
from es_ctrl import EsCtrl
from flask import Flask, jsonify, request, Response
from apscheduler.schedulers.background import BackgroundScheduler

warnings.filterwarnings("ignore")
app = Flask(__name__)
cf = configparser.ConfigParser()
cf.read('config/config.cfg')

env = 'DEVELOP'
if 'win' in platform:
    env = 'DEVELOP'
elif 'linux' in platform:
    env = 'PRODUCT'

@app.after_request
def apply_caching(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
    return response


@app.route("/query_index_logs", methods=['GET'])
def query_index_logs():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        index =  cf['MAIN']['PREFIX'] + request.args.get('index')
        response = jsonify({'content': es_ctrl.query_index(index)})
        return response
    return jsonify({'content': 'error'})


@app.route("/query_key_values", methods=['GET'])
def query_key_values():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        story_line = {}
        origin_index = {}
        inverted_index_table = {}
        for index in request.args.get('index').split(','):
            tmp = es_ctrl.query_index(cf['MAIN']['PREFIX'] + index)
            data = tmp['story_line']
            story_line[index] = {}
            origin_index[index] = {}
            inverted_index_table[index] = {}
            for dev in data.keys():
                story_line[index][dev] = {}
                origin_index[index][dev]  = {}
                inverted_index_table[index][dev] = tmp['inverted_index_table'][dev]
                for process in data[dev]:
                    story_line[index][dev][process['process']] = process['kv']
                    origin_index[index][dev][process['process']]  = list(process['msg'].keys())

        response = jsonify({'story_line': story_line, 'origin_index': origin_index, 'inverted_index_table':inverted_index_table})
        return response
    return jsonify({'content': 'error'})


@app.route("/query_indices", methods=['GET'])
def query_indices():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        response = jsonify({'content': es_ctrl.query_indices()})
        return response
    return jsonify({'content': 'error'})


@app.route("/query_running_indices", methods=['GET'])
def query_running_indices():
    if request.method == 'GET':
        res = list(queue_check.keys()) + queue_running
        response = jsonify({'content': res})
        return response


@app.route("/post_log", methods=['POST'])
def post_log():
    if request.method == 'POST':
        file = request.files['file']
        path = cf['ENV_'+env]['ORIGIN_FILE_STORE_PATH']+file.filename
        file.save(path)

        name = file.filename
        if '.zip' in name:
            name = name
        else:
            name = name.split('.')[0]

        flag = False
        if '.zip' in file.filename:
            flag, filenames = is_dcgm_zip(path, name)
            if  flag != True:
                return Response("Log File Format ERROR!", status=400)
            else:
                for file_name in filenames:
                    queue_check[file_name] = {'check': 0, 'count': 0, 'status': 'running'}
        
        if flag == False:
            flag, filenames = is_multi_devices_telog_log(path, name)
            if  flag == True:
                for file_name in filenames:
                    queue_check[file_name] = {'check': 0, 'count': 0, 'status': 'running'}

        if flag == False:
            flag, file_name = is_single_device_telog_log(path, name)
            if flag == True:
                queue_check[file_name] = {'check': 0, 'count': 0, 'status': 'running'}

        if flag == False:
            flag, file_name = is_lab_log(path, name)
            if flag == True:
                queue_check[file_name] = {'check': 0, 'count': 0, 'status': 'running'}

        if flag == False:
            return Response("Log File Format ERROR!", status=400)

        response = jsonify({'content': 'ok'})
    return jsonify({'content': 'error'})


queue_check = {}
# queue_running = ['visby_6626_dcgm_xiaobo_glt_sukamulya_cbn_cm_bxp_2051_telog',
# 'visby_6626_dcgm_xiaobo_le_pluis_if_bxp_2048_telog'
# ]
queue_running = []

def scheduled_check_queue():
    es_ctrl = EsCtrl()
    print(queue_check)
    if len(queue_check) > 0:
        qs = list(queue_check.keys())
        for key in qs:
            if es_ctrl.is_exists(key) == True:
                count_index = es_ctrl.count_index(key)
                if count_index != 0:
                    if count_index > queue_check[key]['count']:
                        queue_check[key]['count'] = count_index
                        continue
                    else:
                        if queue_check[key]['check'] == int(cf['MAIN']['CHECK']):
                            queue_running.append(key)
                            queue_check.pop(key, None)
                        else:
                            queue_check[key]['check'] = queue_check[key]['check'] + 1


def scheduled_running_queue():
    for _ in range(0, len(queue_running)):
        task = queue_running.pop()
        print('Running log analysis:', task)
        execute(task)


scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_check_queue, trigger="interval", seconds=5)
scheduler.add_job(func=scheduled_running_queue, trigger="interval", seconds=3)
scheduler.start()


def execute(index):
    es_ctrl = EsCtrl()
    story = es_ctrl.query_index_logs(index)
    data = clean_data(story)
    res = es_ctrl.store_index('.analyzed_'+index, data)
    print(res)


atexit.register(lambda: scheduler.shutdown(wait=False))
if __name__ == '__main__':
      app.run(host='0.0.0.0', port=8000)