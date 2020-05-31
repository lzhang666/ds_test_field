#!bin/bash
for ((i=1;i<11;i++))
do
 # pytest test/replication_test.py | grep passed
 pytest test/replication_test.py
 sleep 7
 # OUTPUT = $(pytest test/replication_test.py) 
 # FAIL = $( $OUTPUT | grep passed)
 # echo $FAIL
 # if [ $FAIL ]
 # then
 #  echo $OUTPUT
 # fi
done