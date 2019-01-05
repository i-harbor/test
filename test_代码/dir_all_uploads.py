from urllib.parse import urljoin
import json
import os
import requests
web="http://159.226.227.2"
test_API="/api/v1/auth-token/"
#获取token
post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':'','password':''})
text=json.loads(post_text.text)
token1=text['token']
token=token1['key']
print(token)
#创建一个桶
#bucket_post=requests.post(urljoin(web,'/api/v1/buckets/'),data={'name':'string11'},headers= {'Authorization':'Token '+token})
##print(bucket_post.text)
#待测试的文件
g = os.walk(r"e:\test")  

for path,dir_list,file_list in g:  
    for file_name in file_list:  
        print(os.path.join(path, file_name) )

        files={'chunk':open(os.path.join(path, file_name),'rb')}
        fsize=os.path.getsize(os.path.join(path, file_name))

#循环上传1m图片

        r="%s" % file_name
        files={'chunk':open(os.path.join(path, file_name),'rb')}
        upload_API=requests.put(urljoin(web,'/api/v1/obj/string11/'+r+'/'),files=files, data={"chunk_offset": 0,"chunk_size":fsize},headers= {'Authorization':'Token '+token})
        new=upload_API.text
        new1=json.loads(new)
        print(new1)   
