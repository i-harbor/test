# upload and download directory to evharbor parallel(by python3)

通过evharbor的API，上传本地目录到对象存储，或者从对象存储下载目录到本地.

## upload directory to evharbor parallel

运行示例：01_directory_upload_parallel.py -s /local/directory/src -d /bucket/objfolder1/objfolder2/dst -k xxxxxxxx -t 8

通过evharbor的API，并发上传一个目录到对象存储中.程序执行过程如下：

- 相关参数：线程数目，源路径，目的路径，用户token，通过参数传入；evharbor地址直接在python3代码中配置。 

- 扫描本地目录/local/directory/src，并将元数据写入记录到/local/directory/src目录下 .evharbor.sqlte3 数据库中。元数据只记录相对路径。
  例如 /local/directory/src/1.file，元数据记录 1.file,上传后在对象存储中路径 /bucket/objfolder1/objfolder2/dst/1.file

- 根据.evharbor.sqlte3 数据库中元数据信息，并发上传相关文件。状态有三种：0（未处理），1（处理中），2（处理完成）

- 如果程序中断，下次启动检测到.evharbor.sqlte3，则首先扫描目录更新元数据，对于已经处理完成的跳过
