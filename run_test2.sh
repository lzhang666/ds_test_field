#!bin/bash
for ((i=1;i<11;i++))
do
 # pytest test/election_test.py | grep passed
 pytest test/election_test.py
 sleep 7
 # OUTPUT = $(pytest test/election_test.py) 
 # FAIL = $( $OUTPUT | grep passed)
 # echo $FAIL
 # if [ $FAIL ]
 # then
 #  echo $OUTPUT
 # fi
done