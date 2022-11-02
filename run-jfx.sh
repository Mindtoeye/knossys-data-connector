#!/bin/bash
clear

echo "Run ..."
java -cp "./target/KDataConnector-jar-with-dependencies.jar" --module-path "./target/KDataConnector-jar-with-dependencies.jar" --add-modules javafx.controls,javafx.fxml com.knossys.rnd.data.KDataConnector -d ~/KData 
