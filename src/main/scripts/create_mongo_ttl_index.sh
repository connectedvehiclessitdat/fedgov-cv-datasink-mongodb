#!/bin/bash

source /etc/rtwsrc

# datefield is a date field in the record that is compared against seconds to determine ttl
# i believe the date format should be EEE MMM dd HH:mm:ss z yyyy
# see http://docs.mongodb.org/manual/tutorial/expire-data/
if [ $# -ne 4 ]; then
  echo "usage: create_mongo_ttl_index.sh [dbname] [collection] [datefield] [seconds]"
  exit 1
fi

dbname=$1
collection=$2
datefield=$3
seconds=$4

host=$RTWS_FQDN
port=`cat /etc/mongodb.conf | grep port= | cut -d '=' -f 2`
# only expecting one tll index on one field for now, if multiple fields needed, this will have to change a little
ttl=`mongo $dbname --host $host --port $port --eval "printjson(db.$collection.getIndexes())" | grep expireAfterSeconds | cut -d ":" -f 2`

if [ -z "$ttl" ] || [ "$ttl" -ne "$seconds" ]
then
	if [ -n "$ttl" ] && [ "$ttl" -ne "$seconds" ]
	then
		# doesn't look like mongo lets you easily update the seconds for the index
		# so we are just removing the old index for now if the ttl seconds ever change
		echo "Removing old ttl index for $ttl seconds"
		echo `mongo $dbname --host $host --port $port --eval "printjson(db.$collection.dropIndex( { "$datefield": 1 } ))"`
	fi

	echo "Adding new ttl index for $seconds seconds"
	echo `mongo $dbname --host $host --port $port --eval "printjson(db.$collection.ensureIndex( { "$datefield": 1 }, { expireAfterSeconds: $seconds }  ))"`
fi
