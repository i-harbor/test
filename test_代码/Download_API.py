from urllib.parse import urljoin
import json
import os
import requests
from PIL import Image
from io import BytesIO
web="http://159.226.227.2"
test_API="/api/v1/auth-token/"
Download="/api/v1/obj/sss/700000/"
#获取token
post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':'mazhenwei@cnic.cn','password':'mazhenwei'})
text=json.loads(post_text.text)
token1=text['token']

token=token1['key']
print(token)
#下载任务
Download_API=requests.get(urljoin(web,Download),headers= {'Authorization':'Token '+token,'info':'true'},stream=True)
new2=Download_API.content
path='D:\img'
with open(path,'wb') as f:
    f.write(new2)
print(new2)
##i = Image.open(BytesIO(new2))
##print(i)
