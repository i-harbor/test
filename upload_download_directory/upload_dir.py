from urllib.parse import urljoin
import json
import os
import requests

##API地址
web="http://obs.casearth.cn"
test_API="/api/v1/auth-token/"
bucket='5'               #桶的名字
path='e:/test'                 #需要上传的目录的路径           
chunk_size=1024*1024    #文件分块大小（1M）
chunk_offset=0          #偏移量
       
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
    dir_name=dir_Name[3:]
    create_dir1 = requests.post(urljoin(web, '/api/v1/dir/'), data = { 'bucket_name': bucket,'dir_name': dir_name}, headers = {'Authorization':'Token '+token})

#创建目录
def create_dir(dir_path,dir_name,token):    
    dir_path1=dir_path[3:]#放到根目录下啊
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
    	else:
    		#上传分块大小的块
    		upload = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + bucket_dir + '/' + filename + '/'), files = {'chunk': f.read(chunk_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': chunk_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
    		#偏移量更新
    		chunk_offset = chunk_offset + chunk_size
    		chunk_count = chunk_count + 1			
    f.close()
    #返回总分片数
    return chunk_count
#扫描目录
def search(path,token):
    files=os.listdir(path)
    for file in files:
        a_file=path+'/'+file
        if os.path.isdir(a_file):    #如果是目录
            create_dir(path,file,token)   #创建目录
            search(a_file,token)          #递归扫描
        elif os.path.isfile(a_file):
            dir_path1=path[3:]
            upload_file(file,a_file,bucket,dir_path1,chunk_size, chunk_offset,token)
        else:
            pass
def main():
    #请输入用户名，密码
    username = 'mazhenwei@cnic.cn'
    password = 'mazhenwei'
    
    token = get_token(username, password)
#    create_bucket('444',token)##创建桶的名字
    create_Dir(path,token)
    search(path,token)


if __name__ == '__main__':
    main()
