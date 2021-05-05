import logging

from flask import Flask, jsonify, render_template
import ast
import pymysql
import csv

import compute_recommending_menu
from flask_cors import CORS
from pre_compute import pre_compute_rank

app = Flask(__name__)
CORS(app)
logging.basicConfig(filename="logs/test.log", level=logging.DEBUG)

each_menu_recommend_data = []
each_user_rate_menu_data = []
each_user_pearson_data = []

@app.route('/')
@app.route('/home')
def index():
    return 'Hello, Flask Server!'

@app.route('/refresh_data')
def refresh_data():
    with open('./data/recommend_menu_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for c in reader:
            tempDict = dict()
            tempDict['Menu ID'] = ast.literal_eval(c['Menu ID'])
            tempDict['Pearson Data'] = ast.literal_eval(c['Pearson Data'])
            each_menu_recommend_data.append(tempDict)
    with open('./data/recommend_user_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for c in reader:
            tempDict = dict()
            tempDict['User ID'] = ast.literal_eval(c['User ID'])
            tempDict['Pearson Data'] = ast.literal_eval(c['Pearson Data'])
            each_user_pearson_data.append(tempDict)
    with open('./data/userRatingTable.csv', 'r') as f:
        reader = csv.DictReader(f)
        for c in reader:
            each_user_rate_menu_data.append(c)
    return 'refresh complete'



@app.route('/<int:user_id>')
def recommend(user_id):
    conn = pymysql.connect(host='3.36.135.2',
                           port=3306,
                           user='tkddn2356',
                           password='qwe123012',
                           db='smart_order')

    result = compute_recommending_menu.user_recommend(user_id, each_menu_recommend_data, each_user_pearson_data, conn)
    conn.close()
    return jsonify({"recommend_menus": result})


if __name__ == '__main__':
    refresh_data()
    app.run(debug=False, host='0.0.0.0')
