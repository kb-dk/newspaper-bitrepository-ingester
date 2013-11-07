#!/bin/sh

SCRIPT_DIR="$(dirname "$0")"

java -classpath "$SCRIPT_DIR/../lib/*" -Dlogback.configurationFile=$SCRIPT_DIR/../conf/logback.xml dk.statsbiblioteket.newspaper.bitrepository.ingester.BitrepositoryIngesterExecutable -c $SCRIPT_DIR/../conf/config.properties