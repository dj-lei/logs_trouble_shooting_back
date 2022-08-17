import json
import configparser
from sys import platform
from utils import *
from elasticsearch import Elasticsearch


cf = configparser.ConfigParser()
cf.read('config/config.cfg')

env = 'DEVELOP'
if 'win' in platform:
    env = 'DEVELOP'
elif 'linux' in platform:
    env = 'PRODUCT'


class EsCtrl(object):
    def __init__(self):
        self.es_ctrl = Elasticsearch(cf['ENV_'+env]['ADDR'], ca_certs=cf['ELASTICSEARCH']['CA_CERTS'])

    def query_index_logs(self, index):
        # query = {
        #     "match": {
        #         "trace": "com_ericsson_trithread:INFO"
        #     }
        # }
        #data = self.es_ctrl.search(index=index, query=query, scroll='1s', size=10000)
        data = self.es_ctrl.search(index=index, scroll='1s', size=10000)
        sid = data['_scroll_id']
        scroll_size = len(data['hits']['hits'])
        res = []
        while scroll_size > 0:
            # Before scroll, process current batch of hits
            res.extend(data['hits']['hits'])
            data = self.es_ctrl.scroll(scroll_id=sid, scroll='1s')
            # Update the scroll ID
            sid = data['_scroll_id']
            # Get the number of results that returned in the last scroll
            scroll_size = len(data['hits']['hits'])
        return res

    def query_indices(self):
        res = []
        for key in self.es_ctrl.indices.get_alias().keys():
            if len(key) > 0:
                if '.analyzed_' in key:
                    res.append(key.replace('.analyzed_', ''))
        return res

    def is_exists(self, index):
        return self.es_ctrl.indices.exists(index=index)

    def count_index(self, index):
        return self.es_ctrl.count(index=index)['count']

    def store_index(self, index, data):
        data = deflate_and_base64_encode(json.dumps(data).encode('utf-8'))
        return self.es_ctrl.index(index=index, body={'content': data})

    def query_index(self, index):
        data = self.es_ctrl.search(index=index)
        data = json.loads(decode_base64_and_inflate(data['hits']['hits'][0]['_source']['content']))
        return data