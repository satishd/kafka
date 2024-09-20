#!/bin/bash
result=$(curl -s http://localhost:$UBER_PORT_KAFKA/health)
ret=$?
if [ $ret == 0 ]
then
   if [ $result == OK ]
   then
        echo "OK"
        exit 0
   fi
fi
echo "Healthcheck failed"
exit -1
