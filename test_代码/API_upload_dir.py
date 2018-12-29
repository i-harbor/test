from urllib.parse import urljoin
import json
import os
import requests
web="http://159.226.227.2"
test_API="/api/v1/auth-token/"
#获取token
post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':'mazhenwei@cnic.cn','password':'mazhenwei'})
text=json.loads(post_text.text)
token1=text['token']
token=token1['key']
print(token)

#创建一个桶
bucket_post=requests.post(urljoin(web,'/api/v1/buckets/'),data={'name':'string111'},headers= {'Authorization':'Token '+token})
print(bucket_post.text)

#查询文件夹下所有文件
path1=r"e:\github\test"
g = os.walk(path1)  

#创建一个存储桶下所有的目录
for path,dir_list,file_list in g:  
    for dir_name in dir_list:
        print(os.path.join(path, dir_name) )


#s存储文件名称
s=[]


for path,dir_list,file_list in g:  
    for file_name in file_list:  
#        print(os.path.join( file_name) )
        s.append(os.path.join( file_name))#将文件名称传送给s列表
