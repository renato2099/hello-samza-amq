#!/bin/bash

[[ $JAVA_OPTS != *-Dlog4j.configuration* ]] && export JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=file:$(dirname $0)/log4j-console.xml"

exec $(dirname $0)/run-class.sh org.apache.hadoop.yarn.client.cli.ApplicationCLI application -list | grep application_ | awk -F ' ' '{ print $1 }' | while read linea
do
      $(dirname $0)/kill-yarn-job.sh $linea
done
