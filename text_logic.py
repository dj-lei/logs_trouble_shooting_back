import warnings
from file_operate import FileOperate
from utils import *
from flask import Flask, jsonify, request, Response, make_response
app = Flask(__name__)

@app.after_request
def apply_caching(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,*')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8080')
    return response

files = {}
@app.route("/open_file", methods=['POST'])
def open_file():
    if request.method == 'POST':
        file = request.files['file']
        files[file.filename] = FileOperate(file)
        return jsonify({'lines': files[file.filename].lines, 'inverted_index_table':list(files[file.filename].inverted_index_table.keys())})
    return jsonify({'content': 'error'})

@app.route("/search", methods=['GET'])
def search():
    if request.method == 'GET':
        filename = request.args.get('filename')
        uid = request.args.get('uid')
        desc = request.args.get('desc')
        exp_search = request.args.get('exp_search')
        exp_regex = request.args.getlist('exp_regex[]')
        exp_kv_range = request.args.get('exp_kv_range')
        exp_judge = request.args.get('exp_judge')
        if uid == '':
            uid = files[filename].search(desc, exp_search, exp_regex, exp_kv_range)
        else:
            files[filename].change(uid, desc, exp_search, exp_regex, exp_kv_range)
        return jsonify({'uid': uid, 'content': files[filename].search_atoms[uid].res})
    return jsonify({'content': 'error'})

@app.route("/change", methods=['GET'])
def change():
    if request.method == 'GET':
        filename = request.args.get('filename')
        uid = request.args.get('uid')
        desc = request.args.get('desc')
        exp_search = request.args.get('exp_search')
        exp_regex = request.args.get('exp_regex')
        exp_kv_range = request.args.get('exp_kv_range')
        exp_judge = request.args.get('exp_judge')
        uid = files[filename].search_atoms[uid].change(desc, exp_search, exp_regex, exp_kv_range)
        return jsonify({'uid': uid, 'content': files[filename].search_atoms[uid].res})
    return jsonify({'content': 'error'})

@app.route("/delete_search", methods=['GET'])
def delete_search():
    pass

@app.route("/upload_template", methods=['GET'])
def upload_template():
    pass

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)