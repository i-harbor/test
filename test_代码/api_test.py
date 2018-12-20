#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import requests
import os
import sys
import datetime
from urllib.parse import urljoin

BASE_URL = 'http://159.226.227.2'

#获取Token
def getToken(username, password):
	#get_token = requests.post(urljoin(BASE_URL, '/api/v1/auth-token/'), data = { 'username': username, 'password':password}, params = {'new':'true'})
	get_token = requests.post(urljoin(BASE_URL, '/api/v1/auth-token/'), data = { 'username': username, 'password': password})
	tokendic = json.loads(get_token.text)
	token = 'Token ' + tokendic['token']['key']
	return token

#创建bucket
def createBucket(bucketname, token):
	create_bucket = requests.post(urljoin(BASE_URL, '/api/v1/buckets/'), data = { 'name': bucketname}, headers = {'Authorization':token})
	if create_bucket.status_code == 201 :
		print("创建存储桶", bucketname, "成功！")
	else:
		print("创建存储桶", bucketname, "失败！")
		print(create_bucket.text)
		sys.exit()


#创建目录
def createDir(bucket, dirpath, dirname, token):
	create_dir = requests.post(urljoin(BASE_URL, '/api/v1/dir/'), data = { 'bucket_name': bucket, 'dir_path': dirpath,'dir_name': dirname}, headers = {'Authorization':token})
	if create_dir.status_code == 201 :
		print("在存储桶", bucket, "中创建目录 ", dirpath, "/", dirname, "成功！")
	else :
		print("在存储桶", bucket, "中创建目录 ", dirpath, "/", dirname, "失败！")
		print(create_dir.text)
		sys.exit()


#上传文件
def uploadFile(bucket, dir, filename, token, chunk_size, chunk_offset):
	f = open(filename,'rb')
	fsize = os.path.getsize('./' + filename)  #上传文件的大小
	chunk_count = 0  #分片计数，初始为0
	
	#分片上传
	while chunk_offset < fsize:
		rest_size = fsize - chunk_offset # 文件上传剩余量
		if rest_size < chunk_size:
			#上传剩余量大小的块
			put_obj = requests.put(urljoin(BASE_URL, '/api/v1/obj/'+ bucket + '/' + dir + '/' + filename + '/'), files = {'chunk': f.read(rest_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': rest_size, 'overwrite': True}, headers = {'Authorization':token})
			#偏移量更新
			chunk_offset = chunk_offset + rest_size
			chunk_count = chunk_count + 1
		else:
			#上传分块大小的块
			put_obj = requests.put(urljoin(BASE_URL, '/api/v1/obj/'+ bucket + '/' + dir + '/' + filename + '/'), files = {'chunk': f.read(chunk_size)}, data = {'chunk_offset': chunk_offset, 'chunk_size': chunk_size, 'overwrite': True}, headers = {'Authorization':token})
			#偏移量更新
			chunk_offset = chunk_offset + chunk_size
			chunk_count = chunk_count + 1		
		print(put_obj.text)
		if put_obj.status_code == 200:
			print(f.name, " 分片", chunk_count, "上传成功!")
		else:
			print(f.name, " 分片", chunk_count, "上传失败!")
			sys.exit()

	f.close()
	#返回总分片数
	return chunk_count



def main():
	#用户名，密码
	username = 'your username'
	password = 'your password'
	token = getToken(username, password)

	#存储桶名
	bucket = 'your bucket'

	dir = 'your dir'

	#上传文件
	filename = 'yourfile.txt'
	#上传分块大小（1Mb)
	chunk_size = 1048576
	#上传偏移量
	chunk_offset = 0
	
	#总用时记录
	total_days = 0
	total_seconds = 0
	total_microseconds = 0

	#写日志文件
	with open('log.txt', 'a+') as f:
		f.writelines(['\n' + filename + '\n', '分块大小 ' + str(chunk_size) + '\n', '次序\t分片数\t开始时间\t\t\t结束时间\t\t\t用时\n'])

		#上传3次求时间均值
		for x in range(1, 4):
			starttime =datetime.datetime.now() # 开始时间
			#上传文件
			chunk_count = uploadFile(bucket, dir + '/' + str(x), filename, token, chunk_size, chunk_offset)
			endtime =datetime.datetime.now() # 结束时间
			during = endtime - starttime #计算用时
			#总时间叠加
			total_days = total_days + during.days
			total_seconds = total_seconds + during.seconds
			total_microseconds = total_microseconds + during.microseconds
			#记录本次上传的日志
			f.writelines('\n第' + str(x) + '次\t' + str(chunk_count) + '\t' + str(starttime) + '\t\t\t' + str(endtime) + '\t\t\t' + str(during) + '\n')

		#计算平均时间
		average_days = total_days / x
		average_seconds = total_seconds / x
		average_microseconds = total_microseconds / x
		#时间换算成秒
		average_time = average_days * 24 * 3600 + average_seconds + average_microseconds * 0.000001
		f.writelines('\n平均时间 ' + str(average_days) + '天 ' + str(average_seconds) + '秒 ' + str(average_microseconds) +'微秒'+ '\n')
		f.writelines('\n平均时间 ' +  str(average_time) + '秒' + '\n')




if __name__ == '__main__':
	main()




