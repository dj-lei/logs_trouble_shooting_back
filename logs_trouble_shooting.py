import atexit
import warnings
import configparser
from utils import *
from sys import platform
from drawio import Drawio
from es_ctrl import EsCtrl
from flask import Flask, jsonify, request
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


@app.route("/query_index_logs", methods=['GET'])
def query_index_logs():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        index =  cf['MAIN']['PREFIX'] + request.args.get('index')
        response = jsonify({'content': es_ctrl.query_index(index)})
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
        return response
    return jsonify({'content': 'error'})


@app.route("/query_key_values", methods=['GET'])
def query_key_values():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        res = {}
        print(request.args.get('index'))
        for index in request.args.get('index').split(','):
            data = es_ctrl.query_index(cf['MAIN']['PREFIX'] + index)['story_line']
            res[index] = {}
            for dev in data.keys():
                res[index][dev] = {}
                for process in data[dev]:
                    res[index][dev][process['process']] = process['kv']

        response = jsonify({'content': res})
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
        return response
    return jsonify({'content': 'error'})


@app.route("/query_indices", methods=['GET'])
def query_indices():
    if request.method == 'GET':
        es_ctrl = EsCtrl()
        response = jsonify({'content': es_ctrl.query_indices()})
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
        return response
    return jsonify({'content': 'error'})


@app.route("/query_running_indices", methods=['GET'])
def query_running_indices():
    if request.method == 'GET':
        response = jsonify({'content': queue_check})
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
        return response


@app.route("/post_log", methods=['POST'])
def post_log():
    if request.method == 'POST':
        files = request.files.getlist("file[]")

        for file in files:
        # if 'file' not in request.files:
        #     return jsonify({'content': 'error'})
        # file = request.files['file']
        # # If the user does not select a file, the browser submits an
        # # empty file without a filename.
        # if file.filename == '':
        #     return jsonify({'content': 'error'})
            file.save(cf['ENV_'+env]['LOG_STORE_PATH']+file.filename.lower()+'.log')
            queue_check[file.filename.lower()] = {'check': 0, 'count': 0, 'status': 'running'}

        response = jsonify({'content': 'ok'})
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
        response.headers.add('Access-Control-Allow-Credentials', 'true')
        response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
        return response
    return jsonify({'content': 'error'})


queue_check = {}
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