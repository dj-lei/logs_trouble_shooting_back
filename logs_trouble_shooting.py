import atexit
import warnings
from utils import *
from extract import *
from flask import Flask, jsonify, request, Response, make_response
from apscheduler.schedulers.background import BackgroundScheduler

# warnings.filterwarnings("ignore")
app = Flask(__name__)


@app.after_request
def apply_caching(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
    return response


@app.route("/query_index", methods=['GET'])
def query_index():
    if request.method == 'GET':
        if request.args.get('index') in indices_memory:
            S = indices_memory[request.args.get('index')]
        else:
            index =  cf['ENV_'+env]['LOG_STORE_PATH'] + request.args.get('index')
            with open(index, "rb") as myfile:
                S = myfile.read()
                indices_memory[request.args.get('index')] = S
        response = make_response(S)
        response.headers.add('Content-length', len(S))
        response.headers.add('Content-Encoding', 'gzip')
        return response
    return jsonify({'content': 'error'})


@app.route("/query_indices", methods=['GET'])
def query_indices():
    if request.method == 'GET':
        response = jsonify({'content': indices})
        return response
    return jsonify({'content': 'error'})


@app.route("/query_indices_memory", methods=['GET'])
def query_indices_memory():
    if request.method == 'GET':
        print(indices_memory.keys())
        response = jsonify({'content': {'indices': list(indices_memory.keys())}})
        return response
    return jsonify({'content': 'error'})


@app.route("/query_running_indices", methods=['GET'])
def query_running_indices():
    if request.method == 'GET':
        response = jsonify({'content': queue_running_name})
        return response


@app.route("/post_log", methods=['POST'])
def post_log():
    if request.method == 'POST':
        file = request.files['file']
        path = cf['ENV_'+env]['ORIGIN_FILE_STORE_PATH']+file.filename
        file.save(path)

        name = clean_special_symbols(file.filename,'_') 
        fe = FileExtract(path,name)
        if fe.is_extractable:
            queue_running.append(fe)
            queue_running_name.append(name)
        else:
            return Response("Log File Format ERROR!", status=400)

        return jsonify({'content': 'ok'})
    return jsonify({'content': 'error'})


queue_running = []
queue_running_name = []
indices = []
indices_memory = {}
def scheduled_running_queue():
    for _ in range(0, len(queue_running)):
        task = queue_running.pop()
        print('Running log analysis:', task.filename)
        task.extract()
        for name in task.save_filename:
            indices.append(name)
            with open(cf['ENV_'+env]['LOG_STORE_PATH'] + name, "rb") as myfile:
                S = myfile.read()
            indices_memory[name] = S
        queue_running_name.remove(task.filename)


def scheduled_clear_indices_memory():
    indices_memory = {}


scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_running_queue, trigger="interval", seconds=3)
scheduler.add_job(func=scheduled_clear_indices_memory, trigger="interval", seconds=86400) #after 24 hour, clear indices_memory 
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))
if __name__ == '__main__':
    indices = iterate_files_in_directory(cf['ENV_'+env]['LOG_STORE_PATH'])
    for index in indices:
        if '_'.join(index.split('_')[-3:]) == datetime.datetime.now().strftime("%Y_%m_%d"):
            with open(cf['ENV_'+env]['LOG_STORE_PATH'] + index, "rb") as myfile:
                S = myfile.read()
            indices_memory[index] = S
    app.run(host='0.0.0.0', port=8000)