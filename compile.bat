set JAVA_HOME=C:\Prog\Java\jdk1.8.0_202
set HADOOP_CLASSPATH=%JAVA_HOME%\lib\tools.jar
set HADOOP_HOME=C:\Prog\Hadoop-2.8.0
set Path=%Path%;%HADOOP_HOME%\bin;%JAVA_HOME%\bin

rd /s /q beerReduceBin
md beerReduceBin

call hadoop com.sun.tools.javac.Main -d beerReduceBin src/BeerReduce.java

jar -cvf beerReduceBin/beerReduce.jar -C beerReduceBin/ .