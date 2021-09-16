from operator import itemgetter
import math
import pandas as pd
import numpy as np
import pymysql
import multiprocessing
import time
import csv

np.seterr('raise')
request_result_num = 5
conn = pymysql.connect(host='3.36.135.2',
                       port=3306,
                       user='tkddn2356',
                       password='qwe123012',
                       db='smart_order')
sql_input = "SELECT id, category FROM menu"
menu = pd.read_sql_query(sql_input, conn)

sql_input = "SELECT id FROM user WHERE auth = 1"
user = pd.read_sql_query(sql_input, conn)

sql_input = "select umr.user_id, umr.menu_id, umr.rating, m.category from user_menu_rating umr, menu m where umr.menu_id = m.id;"
data = pd.read_sql_query(sql_input, conn)

process_core = multiprocessing.cpu_count()

menu_length = len(menu)
user_length = len(user)

pivot_matrix = data.pivot_table(index='user_id', columns='menu_id', values='rating')
################################# Pearson Function ##############################################
CATEGORY_WEIGHT = 0.01

def pearsonR(s1, s2):
    s1_c = s1 - s1.mean()
    s2_c = s2 - s2.mean()
    temp = np.sqrt(np.sum(s1_c ** 2) * np.sum(s2_c ** 2))
    if temp == 0:
      return np.sum(s1_c * s2_c) / (temp + 1e-5)
    else:
      return np.sum(s1_c * s2_c) / np.sqrt(np.sum(s1_c ** 2) * np.sum(s2_c ** 2))


def recommend(input_, matrix, n, similar_category=True, isMenu=True):
    if isMenu:
        input_category = menu[menu['id'] == input_]['category'].iloc(0)[0]
        ran = matrix.columns
    else:
        input_category = data[data['user_id'].isin([input_])]['category']
        ran = user['id']
    matrix
    #     카테고리 한개나옴
    result = []
    for name in ran:
        same_count = 0
        if name == input_:
            continue

        # rating comparison
        cor = pearsonR(matrix.loc[input_], matrix.loc[name])

        # genre comparison
        if similar_category:
            if isMenu:
                temp_category = menu[menu['id'] == name]['category'].iloc(0)[0]
                same_count = np.sum(np.isin(input_category, temp_category))
            else:
                temp_category = data[data['user_id'].isin([name])]['category']
                for i in range(len(data[data['user_id'].isin([input_])]['category'].unique())):
                  input_category_uniq = input_category.unique()
                  temp_category_uniq = temp_category.unique()
                  if input_category_uniq[i] in temp_category_uniq:
                    mae = abs(input_category.value_counts()[input_category_uniq[i]] - temp_category.value_counts()[input_category_uniq[i]])
                    if mae == 0:
                      same_count += 1
                    else:
                      same_count += (0.1)/abs(input_category.value_counts()[input_category_uniq[i]] - temp_category.value_counts()[input_category_uniq[i]])
            cor += (CATEGORY_WEIGHT * same_count)

        if np.isnan(cor):
            continue
        else:
            result.append((name, cor))

    result.sort(key=lambda r: r[1], reverse=True)

    return result[:n]


####################################################################################



def process_recommend(process_num, return_list, matrix, isMenu=True):
    rating_num = 3
    if isMenu:
        length = menu_length
    else :
        length = user_length
    for id in range(process_num + 1, length + 1, process_core):
        refined_pearson_data = recommend(id, matrix, rating_num,
                                         similar_category=True, isMenu=isMenu)
        func_result_dict = dict()
        if isMenu:
            func_result_dict['Menu ID'] = id
            func_result_dict['Pearson Data'] = refined_pearson_data
        else:
            func_result_dict['User ID'] = id
            func_result_dict['Pearson Data'] = refined_pearson_data
        return_list.append(func_result_dict)
    # 여기서 csv에 메뉴아이디와 메뉴 랭킹들 리스트 넣음 + 사전형식으로. dict


def pre_compute_rank(isMenu=True):
    if __name__ == '__main__':
        process = []
        start_time = time.perf_counter()
        manager = multiprocessing.Manager()
        result = manager.list()

        for i in range(process_core):
            p = multiprocessing.Process(target=process_recommend, args=(i, result, pivot_matrix, isMenu,))
            process.append(p)
            p.start()

        for p in process:
            p.join()
        if isMenu:
            result = sorted(result, key=itemgetter('Menu ID'))
            with open('./data/recommend_menu_data.csv', 'w', newline='') as f:
                fieldnames = ['Menu ID', 'Pearson Data']
                wt = csv.DictWriter(f, fieldnames)
                wt.writeheader()
                wt.writerows(result)
        else:
            result = sorted(result, key=itemgetter('User ID'))
            with open('./data/recommend_user_data.csv', 'w', newline='') as f:
                fieldnames = ['User ID', 'Pearson Data']
                wt = csv.DictWriter(f, fieldnames)
                wt.writeheader()
                wt.writerows(result)
        end_time = time.perf_counter()
        print("Data Compute Complete, Working time : ", end_time - start_time)
if __name__ == '__main__':
  pre_compute_rank(True)
  pre_compute_rank(False)
