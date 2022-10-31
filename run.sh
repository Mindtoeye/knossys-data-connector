#!/bin/bash
clear

echo "Build ..."
mvn package

echo "Run ..."
java -cp "./target/KDataConnector-jar-with-dependencies.jar" com.knossys.rnd.data.KDataConnector -d ~/KData
