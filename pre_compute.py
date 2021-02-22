from operator import itemgetter

import pandas as pd
import numpy as np
import pymysql
import multiprocessing
import time
import csv


request_result_num = 5
conn = pymysql.connect(host='3.36.135.2',
                       port=3306,
                       user='tkddn2356',
                       password='qwe123012',
                       db='smart_order')
sql_input = "SELECT id, category FROM menu"
meta = pd.read_sql_query(sql_input, conn)

sql_input = "select umr.user_id, umr.menu_id, umr.rating, m.category from user_menu_rating umr, menu m where umr.menu_id = m.id;"
data = pd.read_sql_query(sql_input, conn)

process_core = multiprocessing.cpu_count()

menu_length = len(meta)

menu_matrix = data.pivot_table(index='user_id', columns='menu_id', values='rating')
################################# Pearson Function ##############################################
CATEGORY_WEIGHT = 0.14


def pearsonR(s1, s2):
    s1_c = s1 - s1.mean()
    s2_c = s2 - s2.mean()
    return np.sum(s1_c * s2_c) / np.sqrt(np.sum(s1_c ** 2) * np.sum(s2_c ** 2))


def recommend(input_menu, menu_matrix, n, similar_category=True):
    input_category = meta[meta['id'] == input_menu]['category'].iloc(0)[0]
    #     카테고리 한개나옴

    result = []
    for name in menu_matrix.columns:
        if name == input_menu:
            continue

        # rating comparison
        cor = pearsonR(menu_matrix[input_menu], menu_matrix[name])

        # genre comparison
        if similar_category:
            temp_category = meta[meta['id'] == name]['category'].iloc(0)[0]
            same_count = np.sum(np.isin(input_category, temp_category))
            cor += (CATEGORY_WEIGHT * same_count)

        if np.isnan(cor):
            continue
        else:
            result.append((name, cor))

    result.sort(key=lambda r: r[1], reverse=True)

    return result[:n]


####################################################################################



def process_recommend(process_num, return_list):
    rating_num = 3

    for menuid in range(process_num + 1, menu_length + 1, process_core):
        refined_pearson_data = recommend(menuid, menu_matrix, rating_num,
                                         similar_category=True)
        func_result_dict = dict()
        func_result_dict['Menu ID'] = menuid
        func_result_dict['Pearson Data'] = refined_pearson_data
        return_list.append(func_result_dict)
    # 여기서 csv에 메뉴아이디와 메뉴 랭킹들 리스트 넣음 + 사전형식으로. dict


def pre_compute_rank():
    if __name__ == '__main__':
        process = []
        start_time = time.perf_counter()

        manager = multiprocessing.Manager()
        result = manager.list()

        for i in range(process_core):
            p = multiprocessing.Process(target=process_recommend, args=(i, result))
            process.append(p)
            p.start()

        for p in process:
            p.join()

        result = sorted(result, key=itemgetter('Menu ID'))
        with open('./data/recommend_menu_data.csv', 'w', newline='') as f:
            fieldnames = ['Menu ID', 'Pearson Data']
            wt = csv.DictWriter(f, fieldnames)
            wt.writeheader()
            wt.writerows(result)
        end_time = time.perf_counter()
        print("Data Compute Complete, Working time : ", end_time - start_time)
