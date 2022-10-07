import atexit
import warnings
from utils import *
from extract import *
from flask import Flask, jsonify, request, Response
from apscheduler.schedulers.background import BackgroundScheduler

warnings.filterwarnings("ignore")
app = Flask(__name__)


@app.after_request
def apply_caching(response):
    # response.headers.add('Content-Encoding', 'gzip')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
    return response

# index = cf['ENV_'+env]['LOG_STORE_PATH'] + 'EXIOSUU_GLTE_MALABAR_PL_2051_telog_radio6626_BXP_2051'
# with open(index, "rb") as myfile:
#     S = myfile.read()
# ressult = json.loads(decode_base64_and_inflate(S))
@app.route("/query_index", methods=['GET'])
def query_index():
    if request.method == 'GET':
        index =  cf['ENV_'+env]['LOG_STORE_PATH'] + request.args.get('index')
        with open(index, "rb") as myfile:
            S = myfile.read()
        return jsonify({'content': json.loads(decode_base64_and_inflate(S))})
        # return jsonify({'content': ressult})
    return jsonify({'content': 'error'})


@app.route("/query_indices", methods=['GET'])
def query_indices():
    if request.method == 'GET':
        response = jsonify({'content': indices})
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
def scheduled_running_queue():
    for _ in range(0, len(queue_running)):
        task = queue_running.pop()
        print('Running log analysis:', task.filename)
        task.extract()
        for name in task.save_filename:
            indices.append(name)
        queue_running_name.remove(task.filename)
        


scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_running_queue, trigger="interval", seconds=3)
scheduler.start()
atexit.register(lambda: scheduler.shutdown(wait=False))
if __name__ == '__main__':
    indices = iterate_files_in_directory(cf['ENV_'+env]['LOG_STORE_PATH'])
    app.run(host='0.0.0.0', port=8000)