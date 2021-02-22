import logging

from flask import Flask, jsonify
import ast
import pymysql
import csv
import compute_recommending_menu

app = Flask(__name__)

logging.basicConfig(filename="logs/test.log", level=logging.DEBUG)
conn = pymysql.connect(host='3.36.135.2',
                       port=3306,
                       user='tkddn2356',
                       password='qwe123012',
                       db='smart_order')

each_menu_recommend_data = []
each_user_rate_menu_data = []


@app.route('/$%refresh_data')
def refresh_data():
    with open('./data/recommend_menu_data.csv', 'r') as f:
        reader = csv.DictReader(f)
        for c in reader:
            tempDict = dict()
            tempDict['Menu ID'] = ast.literal_eval(c['Menu ID'])
            tempDict['Pearson Data'] = ast.literal_eval(c['Pearson Data'])
            each_menu_recommend_data.append(tempDict)

    with open('./data/userRatingTable.csv', 'r') as f:
        reader = csv.DictReader(f)
        for c in reader:
            each_user_rate_menu_data.append(c)


refresh_data()


@app.route('/')
@app.route('/home')
def home():
    return 'Hello, Flask Server!'


@app.route('/<int:user_id>')
def recommend(user_id):
    result = compute_recommending_menu.user_recommend(user_id, each_menu_recommend_data, conn)
    return jsonify({"recommend_menus": result})


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
