set JAVA_HOME=%1
set HADOOP_CLASSPATH=%JAVA_HOME%\lib\tools.jar
set HADOOP_HOME=%~dp0hadoop-2.8.0
set Path=%Path%;%HADOOP_HOME%\bin;%JAVA_HOME%\bin

rd /s /q darkestBeerResult

hadoop jar beerReduceBin/darkestBeer.jar DarkestBeer data/recipeData.csv darkestBeerResult