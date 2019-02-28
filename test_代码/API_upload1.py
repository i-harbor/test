from urllib.parse import urljoin
import json
import os
import requests
import sys,getopt
web="http://obs.casearth.cn"
test_API="../api/v1/auth-token/"
print(urljoin(web,test_API))
#获取token
####post_text=requests.post(urljoin(web,test_API),data={'version':'v1','username':'','password':''})
####text=json.loads(post_text.text)
####token1=text['token']
####token=token1['key']
####print(token)
#创建一个桶
def t(bucket,token):

    bucket_post=requests.post(urljoin(web,'/api/v1/buckets/'),data={'name':bucket},headers= {'Authorization':'Token '+token})
##print(bucket_post.text)
#待测试的文件
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
    print(dst,token,src)
def usage():
    print ('''    程序使用说明如下：
    00_single_big_parallel.py [option][value]...
    Example: ./00_single_big_parallel.py -s "/root/1.file" -d "/bucket_name/objstorepath/1.file" -k "xxxxxxxx"
    -h or --help
    -s or --src="文件源路径"，
    -d or --dst="桶名称",
    -k or --token="认证token"
    ''')


def main1(src,bucket,token):

    fsize=os.path.getsize(src)

#循环上传1m图片
    for i in range(1,100000001):
        r="%d" %i
        files={'chunk':open(src,'rb')}
        upload_API=requests.put(urljoin(web,'/api/v1/obj/'+bucket+'/'+r+'/'),files=files, data={"chunk_offset": 0,"chunk_size":fsize},headers= {'Authorization':'Token '+token})
        new=upload_API.text
        new1=json.loads(new)
        print(new1)   
if __name__=='__main__':
    
    GetOpt(sys.argv[1:])
    print(dst,token,src)
    t(dst,token)
    main1(src,dst,token)
