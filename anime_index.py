#coding=UTF-8
import pandas as pd 
import numpy as np 

index_data = pd.read_csv('data_test.csv')
index_data = index_data[['season_id']]
index_data = index_data.to_numpy()
index_data = index_data.T
index_data = np.sort(index_data)
index_data = index_data[0]
index_data_size = index_data.size

def anime_index(season_id):
    '''
    输入season_id 返回其简化序列
    '''
    index = np.where(index_data == season_id)
    return index[0][0]

def has_anime_index(season_id):
    '''
    输入season_id 返回其是否存在于数组中
    '''
    return season_id in index_data