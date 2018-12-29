from urllib.parse import urljoin
import json
import os
import requests

web="http://159.226.227.2"
test_API="/api/v1/auth-token/"


##获取token
def get_token(username, password):
    post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':username,'password':password})
    text=json.loads(post_text.text)
    token1=text['token']
    token=token1['key']
    return token
#下载
def download(Download,token,path):
    Download_API=requests.get(urljoin(web,Download),headers= {'Authorization':'Token '+token,'info':'true'},stream=True)
    new2=Download_API.content
    
    with open(path,'wb') as f:
        f.write(new2)
    print(new2)


def main():
    #请输入用户名，密码
    username = 'mazhenwei@cnic.cn'
    password = 'mazhenwei'
    Download="/api/v1/obj/"+ "sss/10000000/"    #路径＋文件名/
    path=''     #下载文件保存的路径
 
    token = get_token(username, password)
    download(Download,token,path)



if __name__ == '__main__':
    main()
