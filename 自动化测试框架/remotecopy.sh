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
