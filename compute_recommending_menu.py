import pandas as pd
import numpy as np
import multiprocessing
from datetime import datetime, timedelta



def process_recommend(ratings_matrix, each_menu_recommend_data, return_list):
    menu_id_idx = 0
    rating_idx = 1
    each_menu_id_idx = 0
    each_pearson_data_idx = 1
    func_result = []

    for user_rating in ratings_matrix:
        cur_menu_id = user_rating[menu_id_idx]
        csv_menu_idx = cur_menu_id - 1
        cur_menu_rating = user_rating[rating_idx]
        cur_pearson_datas = each_menu_recommend_data[csv_menu_idx]['Pearson Data']
        refined_datas = [(x[each_menu_id_idx], x[each_pearson_data_idx] * cur_menu_rating) for x in cur_pearson_datas]
        func_result.append(refined_datas)

    return_list.append(func_result)

def gather_menu(user_id, pearson, each_menu_recommend_data, return_list, conn, isChild=False):
    if isChild:
        tier1_weight = 2.2
        tier2_weight = 1.8
    else:
        tier1_weight = 3
        tier2_weight = 2.6

    sql_input_count = "select menu_id, count(menu_id) from payment where user_id = " + str(
        user_id) + " group by user_id, menu_id order by count(menu_id) desc"
    user_favor = pd.read_sql_query(sql_input_count, conn)

    pearson_datas = each_menu_recommend_data[user_favor['menu_id'][0] - 1]['Pearson Data']
    refined_datas = []
    for x in pearson_datas:
        temp = user_favor[user_favor['menu_id'] == x[0]]
        if not temp.empty:
            refined_datas.append((x[0], x[1] * pearson * tier2_weight * temp['count(menu_id)'].values[0]))
        else:
            no_count_weight = 0.8
            refined_datas.append((x[0], x[1] * pearson * tier2_weight * no_count_weight))
    func_result = []
    refined_datas.append((user_favor['menu_id'][0], pearson * tier1_weight * user_favor['count(menu_id)'][0]))
    print(refined_datas)
    func_result.append(refined_datas)

    return_list.append(func_result)



def multiprocessing_recommend(ratings_matrix, each_menu_recommend_data, result):
    process_core = multiprocessing.cpu_count()
    process = []
    # func_start_time = time.perf_counter()

    for i in range(process_core):
        p = multiprocessing.Process(target=process_recommend,
                                    args=(ratings_matrix[i::process_core], each_menu_recommend_data, result))
        process.append(p)
        p.start()

    for p in process:
        p.join()

    # func_end_time = time.perf_counter()

    # print("멀티프로세싱. : ", func_end_time - func_start_time)


def don_dup(request_list, request_result_element_num, element_num=0):
    result = []

    for x in request_list:
        if len(result) == request_result_element_num:
            break
        else:
            flag = True
            for i in result:
                if i == x[element_num]:
                    flag = False
            if flag:
                result.append(x[0])

    return result

# 로직 바꿔야 함.
def user_recommend(user_id, each_menu_recommend_data, each_user_pearson_data, conn):
    sql_input = "SELECT id, created_at FROM user WHERE auth = 1 and id = " + str(user_id)
    user = pd.read_sql_query(sql_input, conn)
    user_create_date = user['created_at'][0].to_pydatetime()
    now_date = dt.now()
    if (user.empty || ((((now_date - user_create_date).seconds) / 3600) < 16)):
        result_list = non_user_recommend_func(user_id, each_menu_recommend_data, conn)
    else:
        result_list = user_recommend_func(user_id, each_menu_recommend_data, each_user_pearson_data, conn)

    return result_list

def user_recommend_func(user_id, each_menu_recommend_data, each_user_pearson_data, conn):
    result_list = []
    request_result_num = 5
    cur_user_pearson_datas = each_user_pearson_data[user_id - 501]['Pearson Data']
    gather_menu(user_id, 0.4, each_menu_recommend_data, result_list, conn)
    for user_pearson in cur_user_pearson_datas:
        gather_menu(user_pearson[0], user_pearson[1], each_menu_recommend_data, result_list, conn, True)
    result = np.array(result_list).reshape(-1, 2).tolist()
    result.sort(key=lambda x: x[1], reverse=True)
    final_result = don_dup(request_list=result, request_result_element_num=request_result_num)

    return final_result

def non_user_recommend_func(user_id, each_menu_recommend_data, conn):
    manager = multiprocessing.Manager()
    func_result = manager.list()
    request_result_num = 5
    sql_input = "SELECT user_id, menu_id, rating FROM user_menu_rating where user_id = " + str(
        user_id) + " order by updated_at"
    ratings = pd.read_sql_query(sql_input, conn)
    ratings = ratings[['menu_id', 'rating']]
    ratings_matrix = ratings.values.tolist()
    if len(ratings_matrix) > 69:
        if __name__ == '__main__':
            multiprocessing_recommend(ratings_matrix, each_menu_recommend_data, func_result)
    else:
        # func_start_time = time.perf_counter()

        process_recommend(ratings_matrix, each_menu_recommend_data, func_result)

        # func_end_time = time.perf_counter()
        # print("싱글프로세싱. : ", func_end_time - func_start_time)

    func_result = np.array(func_result).reshape(-1, 2).tolist()
    func_result.sort(key=lambda x: x[1], reverse=True)
    final_result = don_dup(request_list=func_result, request_result_element_num=request_result_num)

    return final_result
