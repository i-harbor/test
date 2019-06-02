import json
import os
import requests
import time
j=0
o=0
##生成文件
files={'chunk':(open('02f997dc6dbd3354a3478e6109761194.png','rb'))}
fsize = os.path.getsize('./02f997dc6dbd3354a3478e6109761194.png')
file1=open('./116.txt','a')
time1=time.asctime(time.localtime(time.time()))
file1.write(str(time1))
##file1.close()
##循环上传
for i in range(1,10000001):
    files={'chunk':(open('02f997dc6dbd3354a3478e6109761194.png','rb'))}
    put_text=requests.put('http://10.0.200.2:8000/api/v1/obj/5bfe3fd3d9d24a711680ecc3/',files=files, data = { "bucket_name": "test", "chunk_offset": 0,"chunk_size": fsize}, headers = {'Authorization':'Token '})
    new=put_text.text
    new1=json.loads(new)
    print(new1['bucket_name'],i)

    if new1['bucket_name']=='test':
        j+=1
        if i%100==0:
            print("共计上传",i,'次',"成功次数",j)
            time2=time.asctime(time.localtime(time.time()))
##            file1=open('./116.txt','a')        
            file1.write('%s%d%s%s%d%s'%("\n共计上传",i,'次',"成功次数",j"。本次结束时间："))
            file1.write(str(time2))
file1.close()
'''    else:
        o+=1
      if i%100==0:
            print("\n共计上传",i,'次',"失败次数",o,)
            time3=time.asctime(time.localtime(time.time()))
            file1=open('./115.txt','a')
            file1.write('%s%d%s%s%d%s'%("\n共计上传",i,'次',"失败次数",o,"。本次结束时间："))
            file1.write(str(time3))
            file1.close()     

