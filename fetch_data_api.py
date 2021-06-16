from flask import Flask, Response
from flask_cors import CORS
import subprocess
import requests as http
import json

app = Flask(__name__)
CORS(app)

def responsify(status,message,data={}):
    code = int(status)
    a_dict = {"data":data,"message":message,"code":code}
    try:
        return Response(json.dumps(a_dict), status=code, mimetype='application/json')
    except:
        return Response(str(a_dict), status=code, mimetype='application/json')

def get_tables():
    cmd = 'aws s3 ls s3://dicom-streaming-store-output --profile=dicom-streaming'
    raw = subprocess.check_output(cmd.split(' '))
    tables = [i.split(b'\n')[0].decode() for i in raw.split(b' ') if b'TABLE_DATA' in i]
    return ['https://dicom-streaming-store-output.s3.amazonaws.com/%s' % j for j in tables]

@app.route('/table_data')
def get_table_data():
    read_json_from_url = lambda url: eval(http.get(url).content.decode())
    items = [read_json_from_url(url) for url in get_tables()]
    return responsify(200, 'OK', items)

if __name__ == '__main__':
    app.run(host='localhost', port=6200)