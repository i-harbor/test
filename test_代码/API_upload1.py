from urllib.parse import urljoin
import json
import os
import requests
web="http://obs.casearth.cn"
test_API="../api/v1/auth-token/"
print(urljoin(web,test_API))
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
files={'chunk':open('02f997dc6dbd3354a3478e6109761194.png','rb')}
fsize=os.path.getsize('02f997dc6dbd3354a3478e6109761194.png')

#循环上传1m图片
for i in range(1,100000001):
    r="%d" %i
    files={'chunk':open('02f997dc6dbd3354a3478e6109761194.png','rb')}
    upload_API=requests.put(urljoin(web,'/api/v1/obj/1/'+r+'/'),files=files, data={"chunk_offset": 0,"chunk_size":fsize},headers= {'Authorization':'Token '+token})
    new=upload_API.text
    new1=json.loads(new)
    print(new1)   
