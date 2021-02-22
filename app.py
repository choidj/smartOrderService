import logging

from flask import Flask, jsonify, render_template, Response
import ast
import pymysql
import csv
import cv2
import numpy as np
from tensorflow.python.keras.applications.mobilenet_v2 import preprocess_input
from tensorflow.python.keras.models import load_model

import compute_recommending_menu
from flask_cors import CORS


app = Flask(__name__)
CORS(app)
logging.basicConfig(filename="logs/test.log", level=logging.DEBUG)

facenet = cv2.dnn.readNet('models/deploy.prototxt', 'models/res10_300x300_ssd_iter_140000.caffemodel')
model = load_model('models/mask_detector.model')

cap = cv2.VideoCapture(0)

maskResult = "test"
each_menu_recommend_data = []
each_user_rate_menu_data = []


def gen_frames():
    while True:
        try:
            success, frame = cap.read()  # read the camera frame
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            h, w = frame.shape[:2]

            blob = cv2.dnn.blobFromImage(frame, scalefactor=1., size=(300, 300), mean=(104., 177., 123.))
            facenet.setInput(blob)
            dets = facenet.forward()

            for i in range(dets.shape[2]):
                confidence = dets[0, 0, i, 2]
                if confidence < 0.5:
                    continue

                x1 = int(dets[0, 0, i, 3] * w)
                y1 = int(dets[0, 0, i, 4] * h)
                x2 = int(dets[0, 0, i, 5] * w)
                y2 = int(dets[0, 0, i, 6] * h)

                face = frame[y1:y2, x1:x2]

                face_input = cv2.resize(face, dsize=(224, 224))
                face_input = cv2.cvtColor(face_input, cv2.COLOR_BGR2RGB)
                face_input = preprocess_input(face_input)
                face_input = np.expand_dims(face_input, axis=0)

                mask, nomask = model.predict(face_input).squeeze()

                if mask > nomask:
                    color = (0, 255, 0)
                    label = 'Mask %d%%' % (mask * 100)
                else:
                    color = (0, 0, 255)
                    label = 'No Mask %d%%' % (nomask * 100)

                cv2.rectangle(frame, pt1=(x1, y1), pt2=(x2, y2), thickness=2, color=color, lineType=cv2.LINE_AA)
                cv2.putText(frame, text=label, org=(x1, y1 - 10), fontFace=cv2.FONT_HERSHEY_SIMPLEX, fontScale=0.8,
                            color=color, thickness=2, lineType=cv2.LINE_AA)

            if not success:
                break
            else:
                ret, buffer = cv2.imencode('.jpg', frame)
                frame = buffer.tobytes()
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')  # concat frame one by one and show result
        except:
            yield ("get_frames실패")



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
def index():
    return render_template('index.html')


@app.route('/video_feed')
def video_feed():
    return Response(gen_frames(), mimetype='multipart/x-mixed-replace; boundary=frame')



@app.route('/<int:user_id>')
def recommend(user_id):
    conn = pymysql.connect(host='3.36.135.2',
                           port=3306,
                           user='tkddn2356',
                           password='qwe123012',
                           db='smart_order')

    result = compute_recommending_menu.user_recommend(user_id, each_menu_recommend_data, conn)
    conn.close()
    return jsonify({"recommend_menus": result})


@app.route('/test')
def test():
    return render_template('test.html')



if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
