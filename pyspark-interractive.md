# Lancer des commandes dans la console interractive

```
$ pyspark
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel).
16/12/30 14:53:26 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
16/12/30 14:53:26 WARN Utils: Your hostname, SQLI-UBUNTU resolves to a loopback address: 127.0.1.1; using 192.168.1.96 instead (on interface wlp1s0)
16/12/30 14:53:26 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.0.2
      /_/

Using Python version 3.5.2 (default, Jul  2 2016 17:53:06)
SparkSession available as 'spark'.
>>>titanic_train = sc.textFile("/home/mpean/Documents/formations/cursus-big-data/data-formation-hadoop/data/titanic_train.csv")
>>>titanic_train.take(5)
['PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked', '1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,-,S', '2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)",female,38,1,0,PC 17599,71.2833,C85,C', '3,1,3,"Heikkinen, Miss. Laina",female,26,0,0,STON/O2. 3101282,7.925,-,S', '4,1,1,"Futrelle, Mrs. Jacques Heath (Lily May Peel)",female,35,1,0,113803,53.1,C123,S']

```
