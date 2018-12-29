from urllib.parse import urljoin
import json
import os
import requests
##API地址
web="http://159.226.227.2"
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
def upload_file(filename,filedir,bucket,dir,chunk_size, chunk_offset,token):
    f = open(filedir,'rb')
    fsize = os.path.getsize(filedir)  #上传文件的大小
    chunk_count = 0  #分片计数，初始为0
	
    #分片上传
    while chunk_offset < fsize:
    	rest_size = fsize - chunk_offset # 文件上传剩余量
    	if rest_size < chunk_size:
    		#上传剩余量大小的块
    		put_obj = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + dir + '/' + filename + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
    		#偏移量更新
    		chunk_offset = chunk_offset + rest_size
    		chunk_count = chunk_count + 1
    	else:
    		#上传分块大小的块
    		put_obj = requests.put(urljoin(web, '/api/v1/obj/'+ bucket + '/' + dir + '/' + filename + '/'), files = {'chunk': f.read(chunk_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': chunk_size, 'overwrite': True}, headers= {'Authorization':'Token '+token})
    		#偏移量更新
    		chunk_offset = chunk_offset + chunk_size
    		chunk_count = chunk_count + 1			
    f.close()
    #返回总分片数
    return chunk_count




def main():
    #请输入用户名，密码
    username = ''
    password = ''

    bucket='string1'               #桶的名字
    filedir='e:/test/333.txt'              #文件路径＋名字
    filename='333.txt'
    chunk_size=1024*1024    #文件分块大小（1M）
    chunk_offset=0          #偏移量
    dir='1/3/6'
    token = get_token(username, password)
#    create_bucket('444',token)##创建桶的名字
    upload_file(filename,filedir,bucket,dir,chunk_size, chunk_offset,token)



if __name__ == '__main__':
    main()
