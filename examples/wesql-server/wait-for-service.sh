#!/bin/sh
echo "Waiting for $1 at host $2 port $3"
while ! nc -z $2 $3
do sleep 1
printf "-"
done
echo -e "  >> $1 has started"
