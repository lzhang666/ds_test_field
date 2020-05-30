#!bin/bash
for ((i=1;i<101;i++))
do
 # pytest RRMQ-2020-Tests/test/replication_test.py | grep passed
 pytest RRMQ-2020-Tests/test/replication_test.py
 sleep 7
 # OUTPUT = $(pytest RRMQ-2020-Tests/test/replication_test.py) 
 # FAIL = $( $OUTPUT | grep passed)
 # echo $FAIL
 # if [ $FAIL ]
 # then
 #  echo $OUTPUT
 # fi
done