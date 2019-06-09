#! /bin/sh
cd $1
NUM=`ps -ef | grep chrome | grep chrome-linux | grep -v grep | wc -l`
if [ $NUM -gt 0 ]
then
       exit 0
else
       cd $1
       node start_chrome.js &
fi
