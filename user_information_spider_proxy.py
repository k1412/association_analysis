'''
1. 实现代理自动更换
2. 无效vmid区段跳过
3. 有效vmid自动保存
'''
import pymysql
import pandas as pd 
import numpy as np 
import requests
from pandas.io.json import json_normalize
import json
import anime_index
from sqlalchemy import create_engine
import sqlalchemy

#PROXY_POOL_URL = 'http://k1412.top:5555/random' #云端的代理池
PROXY_POOL_URL = 'http://api.xdaili.cn/xdaili-api//greatRecharge/getGreatIp?spiderId=947cd405faaa4dcc9c34dc57029abb83&orderno=YZ201972002405khONp&returnType=2&count=1' #云端的代理池

#数据库连接初始化
connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format("wy-remote", "dovewyjzrn123", "www.k1412.top", "3306", "spiders_test")
engine = create_engine(connect_info)

def get_page(vmid,proxies,page=1):
    '''
    爬取指定id用户的追番信息：
    vmid: 用户的id，范围在1-2亿之间
    page: 第几页追番信息，每页最大显示50~
    return: json类型的文件
    '''
    base_url = "https://api.bilibili.com/x/space/bangumi/follow/list?"
    headers = {
        'Host':'api.bilibili.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1',
        'User_Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
        'Origin': 'https://space.bilibili.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }
    params = {
        'type':'1',
        'follow_status': '0',
        'pn': page,
        'ps': '50',
        'vmid': vmid,
        }
    url = base_url
    try:
        response = requests.get(url,headers=headers,params=params,proxies=proxies,verify=False)
        response.json()
        return response.json()
    except ReferenceError as e:
        print('Error',e.args)

def get_size_code(vmid,proxies):
    '''
    预先进行一次爬取，希望可以以此来减少爬取的次数，因为默认每页15个信息。可大部分人追番数远大于这个数字
    此外在这次爬取中，进行网页是否存在信息，以及信息可读性的判断
    vmid:用户id
    return: size包含信息的大小
            code网站可读状态的返回值(bool值)
    '''
    base_url = "https://api.bilibili.com/x/space/bangumi/follow/list?"
    headers = {
        'Host':'api.bilibili.com',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Upgrade-Insecure-Requests': '1',
        'DNT': '1',
        'User_Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
        'Origin': 'https://space.bilibili.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
    }
    params = {
        'type':'1',
        'follow_status': '0',
        'pn': '1',
        'ps': '50',
        'vmid': vmid,
        }
    url = base_url
    try:
        response = requests.get(url,headers=headers,params=params,proxies=proxies,verify=False)
        code = response.json().get('code')
        if code == 0:
            size = response.json().get('data').get('total')
            return size,True,response.json()
        else : return 0,False,response.json()
    except ReferenceError as e:
        print('Error',e.args)

def get_proxy():
    '''
    从云端的代理池获取有用ip,并进行可用性验证
    '''
    # usable_flag = 0
    # while(usable_flag == 0):
    #     response = requests.get(PROXY_POOL_URL)
    #     if response.status_code == 200:
    #         return response.text
    try:
        response = requests.get(PROXY_POOL_URL)
        if response.status_code == 200:
            a = response.json().get("RESULT")[0].get("ip")
            b =  response.json().get("RESULT")[0].get("port")
            return a+':'+b
    except ReferenceError as e:
        print('Error',e.args)

def parse_one_page(json):
    '''
    将返回的json类型的网站数据，存放到dataframe类型的数据当中
    单独做成模块是可以之后稍加修改提取其他部分信息
    '''
    if json:
        items = json.get('data').get('list')
        items = json_normalize(items)
        web_data = pd.DataFrame(items)
        web_data = web_data[['season_id']]
        web_data = web_data.to_numpy()
        web_data = web_data.T 
        web_data = web_data[0]
    return web_data

def user_information_spider():
    '''
    主函数
    每获得100条有用信息，就以追加的方式写入CSV文件一次，其实好像没什么作用，不用多线程，速度就不会有本质提高
    连续获得20条无用信息，就对vmid进行一次大跳跃，
    保存包含有用信息的vmid,上传到数据库试试怎么样？？？
    '''
    #先请求一个代理
    proxy = get_proxy()
    proxies = {
        'http':'http://'+proxy,
        'https':'https://'+proxy,
    }
    generate_pd_flag = 1
    #用一个while循环代替
    useful_num = 0    #将有用信息数量当作终止循环的条件
    invalid_num = 0
    vmid = 1          #开始爬取的id
    while(useful_num<20):#目标是一万个哇
        if invalid_num >= 100:
            vmid+=200000
        total,code,web_source_data = get_size_code(vmid,proxies)    #web_source_data是从网站获取的json文件
        if not (code and total>0):  #如果code值为flase就跳过这一vmid
            vmid+=1
            invalid_num+=1
            continue
        #single_data = np.zeros(2903, dtype=bool)      #新建一个数组，准备接受返回材料
        single_data = np.zeros(2903)      #新建一个数组，准备接受返回材料
        invalid_num = 0
        vmid_list = pd.Series(vmid)       #保存vmid
        vmid_list.to_csv('vmid_list.csv',mode='a',index=False,header= False)                                                  #保存vmid
        global web_data
        web_data = parse_one_page(web_source_data)     
        total-=50
        page = 2
        while(total>0):
            web_source_data = get_page(vmid,proxies,page)
            web_data = np.append(web_data,parse_one_page(web_source_data))
            page+=1
            total-=50
        useful_num+=1
        vmid+=1
        #对单个有意义数据的处理：
        for iteam in web_data :
            if anime_index.has_anime_index(iteam):
                id_index = anime_index.anime_index(iteam)
                single_data[id_index] = 1

        if generate_pd_flag == 1:
            global anime_list
            anime_list = pd.DataFrame(single_data).T
            generate_pd_flag = 0
        else:
            anime_list_new = pd.DataFrame(single_data).T
            anime_list = pd.concat([anime_list,anime_list_new],ignore_index=True)

        #每多少次就进行一次的操作：：：
        if useful_num%10 == 0:
            print anime_list,useful_num
            anime_list.to_csv('anime_list.csv',mode='a',index=False,header= False)
            anime_list = pd.DataFrame()
            proxy = get_proxy()
            proxies = {
                'http':'http://'+proxy,
                'https':'https://'+proxy,
            }
    
user_information_spider()