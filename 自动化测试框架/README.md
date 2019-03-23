#				参考文档


## 1.主机设置
	master ip：10.XX.XX.87
	client ip:10.XX.XX.225-239



## 2.分别通过master对client进行免密登陆
### 具体步骤如下：
       	1）在master执行 ssh-keygen -t rsa 命令，生成密钥文件
	2）master执行ssh-copy-id -i ~/.ssh/id_rsa.pub dss@10.XX.XX.225-239（分别执行，将公钥文件传输到client）
	3）执行生效查看.ssh文件是否有新的公钥生成



## 3.创建文件的同步系统
	1）将master的文件自动化上传到client上
	2）设置client主机列表（/updata）
	3）执行自动化脚本（remmotecopy.sh），此脚本参数  client IP列表    本地文件目录     client存储目录
		eg：sh remotecopy.sh -f /updata/hosts /root/API_upload1.py /root/API_upload2.py
	4）脚本执行完后自动关闭，确认同步成功	
```
#!/bin/bash
while getopts f: OPT;
do
	case $OPT in
		f|+f)
			files="$OPTARG $files"
			;;
		*)
			echo "usage: `basename $0` [-f hostfile] <from> <to>"
			exit 2
	esac
done
shift `expr $OPTIND - 1`
 
if [ "" = "$files" ];
then
	echo "usage: `basename $0` [-f hostfile] <from> <to>"
	exit
fi
 
for file in $files
do
	if [ ! -f "$file" ];
	then
		echo "no hostlist file:$file"
		exit
fi
hosts="$hosts `cat $file`"
done
 
for host in $hosts;
do
	echo "do $host"
	scp $1 root@$host:$2
done
```


## 4.创建并行执行linux系统命令系统
	1）master控制client并行执行master所要执行的命令行
	2）设置client主机列表（/updata）
	3）执行自动化脚本（doCommand.sh）此脚本参数为要执行的命令要用‘ ’引用住
		eg： sh doCommand.sh 'python36 API_upload2.py >/dev/null 2>&1 &'
		（并行计算需要在命令后加 /dev/null 2>&1 &,目的在于将返回的参数放入linux临时存储的空间以达到并行执行命令的目的，大大减少执行时间）
```
#!/bin/sh

doCommand()
{
    hosts=`sed -n '/^[^#]/p' hostlist`
    for host in $hosts
        do
            echo ""
            echo HOST $host
            ssh $host "$@"
        done
    return 0
}

    if [ $# -lt 1 ]
    then
            echo "$0 cmd"
            exit
    fi
    doCommand "$@"
    echo "return from doCommand"

```


注：所需文件脚本在本框架文件夹下。







