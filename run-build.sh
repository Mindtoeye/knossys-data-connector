#!/bin/bash
clear

echo "Build ..."
mvn package
echo "Assembling ..."
mvn compile assembly:single