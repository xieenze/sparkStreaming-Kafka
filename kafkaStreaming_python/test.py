import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer
consumer = KafkaConsumer('result')
def test():
    girl = 0
    boy = 0
    for msg in consumer:
        data_json = msg.value.decode('utf8')
        data_list = json.loads(data_json)
        for data in data_list:
            if "0" in data.keys():
                girl = data['0']
            elif "1" in data.keys():
                boy = data['1']
            else:
                continue
        result = str(girl) + ',' + str(boy)
        print(result)

test()