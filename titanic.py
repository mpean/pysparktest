# spark-submit titanic.py

from pyspark import SparkConf, SparkContext
import logging
from tools import logRDD, initDataDir, initSparkContext

baseDir, dataInDir, dataOutDir, logFile = initDataDir("/titanic.log") # initialisation de l'arbo DATA et du fichier de log
logging.basicConfig(filename=logFile, filemode='w', level=logging.INFO, format='%(asctime)s %(message)s') # configuration des logs
conf, sc = initSparkContext() # configuration du spark context

logging.info('Started')

# ouverture du fichier
titanic_train_rdd = sc.textFile(dataInDir + "/titanic_train.csv")
logRDD("contenu du fichier titanic_train.csv", titanic_train_rdd)

# suppression de la ligne contenant le nom des champs
titanic_train_data = titanic_train_rdd.filter(
    lambda ligne: 'PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked' not in ligne)
logRDD("suppression de la ligne contenant le nom des champs", titanic_train_data)

# split des enregistrements
ttd = titanic_train_data.map(lambda ligne: ligne.split('"'))
logRDD("split des enregistrements", ttd)

# on supprime le nom de la personne.
# Le [:-1] permet de supprimer une virgule en trop.
# Les champs restants sont :
# 'PassengerId,Survived,Pclass,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked'
ttd1 = ttd.map(lambda s: s[0][:-1] + s[-1])
logRDD("on supprime le nom de la personne", ttd1)

# survivants
survivants = ttd1.map(lambda ligne: ligne.split(',')).filter(lambda person: int(person[1]))
logRDD("survivants", survivants)

# survivants hommes
survivants_hommes = survivants.filter(lambda person: person[3] == 'male')
logRDD("survivants hommes", survivants_hommes)

# survivants femmes
survivants_femmes = survivants.filter(lambda person: person[3] == 'female')
logRDD("survivants femmes", survivants_femmes)

# survivants par sexe
survivants_genre = survivants.map(lambda person: (person[3], int(1))).reduceByKey(lambda a, b: a + b)
logRDD("survivants par sexe", survivants_genre)

# top des survivants pas classe
survivants_classe = survivants.map(lambda person: (person[2], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], False)
logRDD("top des survivants pas classe", survivants_classe)

logging.info('Finished')
