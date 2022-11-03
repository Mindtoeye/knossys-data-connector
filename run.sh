#!/bin/bash
clear

if [[ ! -f .env ]]
then
    echo ".env does not exist on your filesystem."
    exit 1
fi

echo "Run ..."

export dbHost="127.0.0.1"
export dbPort="43306"
export dbName="knossys"
export dbUsername="knossys"
export dbTable="portal"
export dbPassword="4570WK821X6OiyT508srN09wV"

java -cp "./target/KDataConnector-jar-with-dependencies.jar" com.knossys.rnd.data.KDataConnector -d ~/kdata 
