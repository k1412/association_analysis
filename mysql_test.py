#新建一个spiders数据库，并创建一个学生数据的数据表：：

# import pymysql

# db = pymysql.connect(host= 'www.k1412.top',user= 'wy-remote', password= 'dovewyjzrn123',port= 3306)
# cursor = db.cursor()
# cursor.execute("CREATE DATABASE spiders_test DEFAULT CHARACTER SET utf8")
# db.close()
# import pymysql

# db = pymysql.connect(host= 'www.k1412.top',user= 'wy-remote', password= 'dovewyjzrn123',port= 3306,db= 'spiders_test')
# cursor = db.cursor()
# sql = 'CREATE TABLE IF NOT EXISTS students (id VARCHAR(255) NOT NULL,name VARCHAR(255) NOT NULL,age INT NOT NULL,PRIMARY KEY (id))'
# cursor.execute(sql)
# db.close()

import pymysql
import pandas as pd 
from sqlalchemy import create_engine
import sqlalchemy
connect_info = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format("wy-remote", "dovewyjzrn123", "www.k1412.top", "3306", "spiders_test")
engine = create_engine(connect_info)

pd_testFile_1 = pd.read_csv('data_test_1.csv')
pd_testFile_2 = pd.read_csv('data_test_2.csv')
# pd_testFile_1.set_index('title',inplace=True)
# pd_testFile_1.rename_axis(None)
# pd_testFile_2.set_index('title',inplace=True)
# pd_testFile_2.rename_axis(None)
print pd_testFile_1
pd_testFile_1.to_sql(
name = 'test7',
con = engine,
if_exists = 'append',
index= False,
dtype={'title':sqlalchemy.types.VARCHAR(length=255),
        'media_id':sqlalchemy.types.INTEGER(),
        'season_id':sqlalchemy.types.INTEGER(),
        'score':sqlalchemy.types.VARCHAR(length=255),
        'follow_num':sqlalchemy.types.VARCHAR(length=255),
        'play_num':sqlalchemy.types.VARCHAR(length=255)
    }
)
pd_testFile_2.to_sql(
name = 'test7',
con = engine,
if_exists = 'append',
index=False,
dtype={'title':sqlalchemy.types.VARCHAR(length=255),
        'media_id':sqlalchemy.types.INTEGER(),
        'season_id':sqlalchemy.types.INTEGER(),
        'score':sqlalchemy.types.VARCHAR(length=255),
        'follow_num':sqlalchemy.types.VARCHAR(length=255),
        'play_num':sqlalchemy.types.VARCHAR(length=255)
    }
)
'''
尝试了将CSV通过pandas中的数据库组件上传到sql远端数据库：
问题：
1. 会将dataframe中未命名的index以unnamed :0的形式保存到第一列，后期引起前面索引的重复，因为在读取sql时，pandas会再为数据增加一个index，所以在数据库中索引暂时的没有用的
2.推荐配置，dataframe不保留index列，或者不进行处理
index = False
'''