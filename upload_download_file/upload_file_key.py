import os
import sys , getopt
import time
from pyharbor import set_global_auth_key, set_global_settings
import pyharbor
set_global_settings({
    'SCHEME': 'http',   # 或'https', 默认'http'
    'DOMAIN_NAME': 'obs.casearth.cn', # 默认 'obs.casearth.cn'
    'ACCESS_KEY': '44aba82a40b',
    'SECRET_KEY': '5141f09',
    })
def GetOpt(argv):
    try:
      
        opts,args=getopt.getopt(argv,"s:d:h",["src=","dst=","help"])
        for opt,value in opts:
            if opt in ("-s" , "--src"):
                global src;
                src = value;
            if opt in ("-d" , "--dst"):
                global dst;
                dst = value;
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
    ''')

def main():
##    file1=open('./log.txt','a')
##    time1=time.time()
    filename = src
    client = pyharbor.get_client()
    ok, offset, msg = client.bucket(dst).dir('').put_object(obj_name=filename, filename=filename)
    if os.path.getsize(filename) == offset:
            
##            d="%d" %(time.time()-time1)
            
            
        print(filename+" "+'\n')

#ok, offset, msg = client.bucket(dst).dir(src).put_object(obj_name=i, filename=filename)
''' 或者
    for i in range(50):
        name="%d" %i
        ok, offset, msg = client.bucket(dst).dir('').put_object(obj_name=name, filename=filename)
        if os.path.getsize(filename) == offset:
            
            d="%d" %(time.time()-time1)
            
            
            print(name+" "+d+'\n')
            file1.write(name+" "+d+'\n')
        else:
          print('上传失败：' + msg)
    file1.colse()
'''
if __name__=='__main__':
    

    GetOpt(sys.argv[1:])

    main()

    
