import pandas as pd
import numpy as np
import multiprocessing
import log



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


def user_recommend(user_id, each_menu_recommend_data, conn):
    manager = multiprocessing.Manager()
    func_result = manager.list()
    request_result_num = 5
    sql_input = "SELECT user_id, menu_id, rating FROM user_menu_rating where user_id = " + str(
        user_id) + " order by updated_at"
    ratings = pd.read_sql_query(sql_input, conn)
    ratings = ratings[['menu_id', 'rating']]
    ratings_matrix = ratings.values.tolist()
    log.log(request, ratings_matrix)
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
