from urllib.parse import urljoin
import json
import os
import requests
import sys ,getopt
import time
##API地址
web="http://obs.casearth.cn"
test_API="/api/v1/auth-token/"

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

 
#上传文件
def upload_file(filename,filedir,bucket,dir,chunk_size, chunk_offset,token,t):
    time2=time.asctime(time.localtime(time.time()))
    f = open(filedir,'rb')
    print('文件打开了',time2)
    fsize = os.path.getsize(filedir)  #上传文件的大小
    chunk_count = 0  #分片计数，初始为0
    while chunk_offset < fsize:
        rest_size = fsize - chunk_offset  # 文件上传剩余量
        if rest_size < chunk_size:
            #上传剩余量大小的块
            print('最后一块了')
            put_obj = requests.put(urljoin(web, '/api/v1/obj/'+ bucket +  dir + '/' + filename+t + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
            #偏移量更新
            chunk_offset = chunk_offset + rest_size
            chunk_count = chunk_count + 1
            print('分片成功')

        else:
            print('第',chunk_count+1,'块开始上传',time.asctime(time.localtime(time.time())))
            #上传分块大小的块
            put_obj = requests.put(urljoin(web, '/api/v1/obj/'+ bucket +  dir + '/' + filename+t + '/'), files = {'chunk': f.read(chunk_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': chunk_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
            #偏移量更新
            chunk_offset = chunk_offset + chunk_size
            chunk_count = chunk_count + 1
            print('第',chunk_count,'块上传成功',time.asctime(time.localtime(time.time())))
    f.close()
    print('文件成功')
    time1=time.asctime(time.localtime(time.time()))
    print(time1)
    #返回总分片数
    return chunk_count
##  python upload_dir_parallel.py -s E:/test -d win -k 97d1cff95c36804c740eb781c17f1446b01528f2  
def GetOpt(argv):
    try:
      
        opts,args=getopt.getopt(argv,"s:d:k:h",["src=","dst=","token=","help"])
        for opt,value in opts:
            if opt in ("-s" , "--src"):
                global src;
                src = value;
            if opt in ("-d" , "--dst"):
                global dst;
                dst = value;
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



def main(filedir,dst,token,t):
    #请输入用户名，密码
##    username = ''
##    password = ''


##    filedir='e:/test/333.txt'              #文件路径＋名字
    global filename
    filename=os.path.basename(filedir)
    chunk_size=1024*1024*100    #文件分块大小（1M）
    chunk_offset=0          #偏移量
    dir=''
##    token = get_token(username, password)
    
    upload_file(filename,filedir,dst,dir,chunk_size, chunk_offset,token,t)



if __name__ == '__main__':

    GetOpt(sys.argv[1:])
    create_bucket(dst,token)##创建桶的名字
    for i in range(100):
        r="%d" %i
        main(src,dst,token,r)
        
