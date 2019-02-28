from urllib.parse import urljoin
import json
import os
import requests
import sqlite3
import threading
import sys ,getopt
Id=0

##API地址
web="http://obs.casearth.cn"
test_API="/api/v1/auth-token/"
##bucket=''               #桶的名字
##path=r''                 #需要上传的目录的路径           

chunk_size=1024*1024    #文件分块大小（1M）
chunk_offset=0          #偏移量

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
def create_Dir(dir_Name,token):
    dir_name=os.path.basename(dir_Name)
    create_dir1 = requests.post(urljoin(web, '/api/v1/dir/'), data = { 'bucket_name': bucket,'dir_name': dir_name}, headers = {'Authorization':'Token '+token})

#创建目录
def create_dir(dir_path,dir_name,token):    
    dir_path1=dir_path[del_len:]
    create_dir1 = requests.post(urljoin(web, '/api/v1/dir/'), data = { 'bucket_name': bucket, 'dir_path': dir_path1,'dir_name': dir_name}, headers = {'Authorization':'Token '+token})
    
 
#上传文件
def upload_file(filename,filedir,bucket,bucket_dir,chunk_size, chunk_offset,token):
    f = open(filedir,'rb')
    fsize = os.path.getsize(filedir)  #上传文件的大小
    chunk_count = 0  #分片计数，初始为0
	
    #分片上传
    while chunk_offset < fsize:
        rest_size = fsize - chunk_offset # 文件上传剩余量
        if rest_size < chunk_size:
            #上传剩余量大小的块
            upload = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + bucket_dir + '/' + filename + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
            #偏移量更新
            chunk_offset = chunk_offset + rest_size
            chunk_count = chunk_count + 1
            print('s')
            for i in range(5):                
                upload = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + bucket_dir + '/' + filename + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
                if upload.status_code==200:
                    break
                    
        else:
            pass
            #上传分块大小的块
            upload = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + bucket_dir + '/' + filename + '/'), files = {'chunk': f.read(chunk_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': chunk_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
            #偏移量更新
            chunk_offset = chunk_offset + chunk_size
            chunk_count = chunk_count + 1
            for i in range(5):                
                upload = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + bucket_dir + '/' + filename + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
                if upload.status_code==200:
                    break

    f.close()
    print('上传成功')
    #返回总分片数
    return chunk_count
#扫描目录，选取目录
def search_dir(path,token):
    
    files=os.listdir(path)
    for file in files:

        a_file=path+'/'+file
        if os.path.isdir(a_file):    #如果是目录
            create_dir(path,file,token)   #创建目录
            search_dir(a_file,token)          #递归扫描        
            
        else:
            pass
    
#扫描目录,选取文件
def search_file(path,token):

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
def read_sql(i,bucket,chunk_size, chunk_offset,token):

    conn=sqlite3.connect('test.db')
    cursor=conn.cursor()
    sql='select * from user'
    q=cursor.execute(sql)
    for r in q:

        if r[0]%5==i:

            
            upload_file(r[1],r[2],bucket,r[3],chunk_size, chunk_offset,token)
 
    cursor.close()
    conn.close()

def GetOpt(argv):
    try:
      
        opts,args=getopt.getopt(argv,"s:d:k:h",["src=","dst=","token=","help"])
        for opt,value in opts:
            if opt in ("-s" , "--src"):
                global path;
                path = value;
            if opt in ("-d" , "--dst"):
                global bucket;
                bucket = value;
            if opt in ("-k" , "--token"):
                global token;
                token = value;
            if opt in ("-h" , "--help"):
                usage();
                sys.exit(-1)
    except getopt.GetoptError as e:
        print (e.msg)
        sys.exit(-1)

def usage():
    print ('''    程序使用说明如下：
    00_single_big_parallel.py [option][value]...
    Example: ./00_single_big_parallel.py -s "/root/1.file" -d "/bucket_name/objstorepath/1.file" -k "xxxxxxxx"
    -h or --help
    -s or --src="文件源路径"，
    -d or --dst="桶名称",
    -k or --token="认证token"
    ''')


def main():


#    create_bucket('444',token)##创建桶的名字
    create_Dir(path,token)
#搜索目录创建，文件存入数据库。
    t1=threading.Thread(target=search_dir,args=(path,token))
    t2=threading.Thread(target=search_file,args=(path,token))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    return token

if __name__ == '__main__':

##    semaphore=threading.Semaphore(5)#多线程上传锁
    GetOpt(sys.argv[1:])
    global path_base
    path_base=os.path.basename(path)
    del_len=len(path)-len(path_base)#删除目录的路径
    main()

    
##    token = get_token(username, password)
    
#创建5个线程

    t3=threading.Thread(target=read_sql,args=(1,bucket,chunk_size, chunk_offset,token))
    t4=threading.Thread(target=read_sql,args=(2,bucket,chunk_size, chunk_offset,token))
    t5=threading.Thread(target=read_sql,args=(3,bucket,chunk_size, chunk_offset,token))
    t6=threading.Thread(target=read_sql,args=(4,bucket,chunk_size, chunk_offset,token))
    t7=threading.Thread(target=read_sql,args=(0,bucket,chunk_size, chunk_offset,token))
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
                    


    
