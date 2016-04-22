# XixianSpongeCity
在运行之前，需要运行以下maven命令，安装eventhub-spout和sqlserver依赖：


mvn install:install-file -Dfile=lib\eventhubs-storm-spout-0.9-jar-with-dependencies.jar -DgroupId=com.microsoft.eventhubs \
    -DartifactId=eventhubs-storm-spout -Dversion=0.9 -Dpackaging=jar
    
mvn install:install-file -Dfile=lib\sqljdbc4-2.0.jar -DgroupId=com.microsoft -DartifactId=sqlserver-jdbc4 -Dversion=2.0 -Dpackaging=jar
