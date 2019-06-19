from urllib.parse import urljoin
import json
import os
import requests
import sqlite3
import threading
import sys ,getopt
import time
from pyharbor import set_global_auth_key, set_global_settings
import pyharbor
set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'http'
    'DOMAIN_NAME': 'obs.casearth.cn', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '44aba82a40bc11e9a16bc8000a00c8d7',
    'SECRET_KEY': '5141f096b9280ca3d14d5b76f324dd062d9d8f34',
    })
Id=0
client = pyharbor.get_client()
##API地址
# web="http://obs.casearth.cn"
# test_API="/api/v1/auth-token/"
##bucket=''               #桶的名字
##path=r''                 #需要上传的目录的路径           

# chunk_size=1024*1024    #文件分块大小（1M）
# chunk_offset=0          #偏移量

#请输入用户名，密码
##username = ''
##password = '' 
##获取token
def get_token(username, password):
    post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':username,'password':password})
    text=json.loads(post_text.text)
    token1=text['token']
    token=token1['key']
    return token

#创建一个桶
def create_bucket(bucket_name,token):
    bucket_post=requests.post(urljoin(web,'/api/v1/buckets/'),data={'name':bucket_name},headers= {'Authorization':'Token '+token})
    print(bucket_post.text)
#创建根目录
def create_Dir(bucket,dir_Name):
    client = pyharbor.get_client()
    ok, msg = client.bucket(bucket).dir().create_dir(dir_Name)
# 或者
# ok, msg = client.create_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
    if ok:
        print('创建成功：' + msg)
    else:
        print('创建失败：' + msg)
#创建目录
def create_dir(bucket,dir_path,dir_name):    
    client = pyharbor.get_client()
    dir_path3=dir_path[del_len:]
    dir_path1=dir_path3.replace("\\","/")
    
    ok, msg = client.bucket(bucket).dir(dir_path1).create_dir(dir_name)
    # 或者
    # ok, msg = client.create_dir(bucket_name='gggg', dir_name='u/rrth/testdir')
    if ok:
        print('创建成功：' + msg)
    else:
        print('创建失败：' + msg)
 
#上传文件
def upload_file(filename,filedir,bucket,bucket_dir):
    ok, offset, msg = client.bucket(bucket).dir(bucket_dir).put_object(obj_name=filename, filename=filedir)
# 或者
# ok, offset, msg = client.put_object(bucket_name='gggg', obj_name='u/rrth/test.py', filename=filename)
    if os.path.getsize(filedir) == offset:
        print('上传成功：' + msg)
    else:
        print('上传失败：' + msg)
#扫描目录，选取目录
def search_dir(path):
    
    files=os.listdir(path)
    for file in files:

        a_file=path+'/'+file
        if os.path.isdir(a_file):    #如果是目录
            create_dir(bucket,path,file)   #创建目录
            search_dir(a_file)          #递归扫描        
            
        else:
            pass
    
#扫描目录,选取文件
def search_file(path):

    g=os.walk(path)
    conn=sqlite3.connect('test.db')#打开数据库
    cursor=conn.cursor()
    for path1,dir_list,file_list in g:
        for file_name in file_list:
            global Id
            Id=Id+1
            file=file_name
            a_file=os.path.join(path1,file_name)
            dir_path2=os.path.join(path1)
            
            dir_path3=dir_path2[del_len:]
            dir_path1=dir_path3.replace("\\","/")

            
            sql='insert into user (id,filename,filedir,bucket_dir) values(?,?,?,?)'
            cursor.execute(sql,(Id,file,a_file,dir_path1))
    cursor.close()
    conn.commit()
    conn.close()
#读取数据库
def read_sql(i,bucket):

    conn=sqlite3.connect('test.db')
    cursor=conn.cursor()
    sql='select * from user'
    q=cursor.execute(sql)
    for r in q:
        print(r[1],r[2],r[3])
        if r[0]%5==i:            
            upload_file(r[1],r[2],bucket,r[3]) 
    cursor.close()
    conn.close()

def GetOpt(argv):
    try:
      
        opts,args=getopt.getopt(argv,"s:d:n:h",["src=","dst=""name=","help"])
        for opt,value in opts:
            if opt in ("-s" , "--src"):
                global patht;
                patht = value;
            if opt in ("-d" , "--dst"):
                global bucket;
                bucket = value;
            if opt in ("-n","--name"):
                global dirname1;
                dirname1=value;
            if opt in ("-h" , "--help"):
                usage();
                sys.exit(-1)
    except getopt.GetoptError as e:
        print (e.msg)
        sys.exit(-1)

def usage():
    print ('''    程序使用说明如下：
    00_single_big_parallel.py [option][value]...
    Example: ./00_single_big_parallel.py -s "/root/1.file" -d "/bucket_name/objstorepath/1.file" -n "xxxxxxxx"
    -h or --help
    -s or --src="文件源路径"，
    -d or --dst="桶名称",
    -n or --name="要上传的文件夹名字"
    ''')


def main():


#    create_bucket('444',token)##创建桶的名字
   create_Dir(bucket,dirname1)
#搜索目录创建，文件存入数据库。
   t1=threading.Thread(target=search_dir,args=(patht,))
   t2=threading.Thread(target=search_file,args=(patht,))
   t1.start()
   t2.start()
   t1.join()
   t2.join()
    

if __name__ == '__main__':

#    semaphore=threading.Semaphore(5)#多线程上传锁
    GetOpt(sys.argv[1:])
    global path_base
    path_base=os.path.basename(patht)
    del_len=len(patht)-len(path_base)#删除目录的路径
    main()
#    read_sql(1,bucket)

    
#    token = get_token(username, password)
    
# 创建5个线程

    t3=threading.Thread(target=read_sql,args=(1,bucket))
    t4=threading.Thread(target=read_sql,args=(2,bucket))
    t5=threading.Thread(target=read_sql,args=(3,bucket))
    t6=threading.Thread(target=read_sql,args=(4,bucket))
    t7=threading.Thread(target=read_sql,args=(0,bucket))
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
                    


    
