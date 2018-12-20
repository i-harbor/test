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
#bucket_post=requests.post(urljoin(web,'/api/v1/buckets/'),data={'name':'string11'},headers= {'Authorization':'Token '+token})
##print(bucket_post.text)
#待测试的文件
files={'chunk':open('02f997dc6dbd3354a3478e6109761194.png','rb')}
fsize=os.path.getsize('./02f997dc6dbd3354a3478e6109761194.png')

#循环上传1m图片
for i in range(361378,10000001):
    r="%d" %i
    files={'chunk':open('02f997dc6dbd3354a3478e6109761194.png','rb')}
    upload_API=requests.put(urljoin(web,'/api/v1/obj/sss/'+r+'/'),files=files, data={"chunk_offset": 0,"chunk_size":fsize},headers= {'Authorization':'Token '+token})
    new=upload_API.text
    new1=json.loads(new)
    print(new1)   
