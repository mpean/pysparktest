"""
outils
"""
import logging
import os
from pyspark import SparkConf, SparkContext

def logRDD(message, rdd, take=5):
    """
    Log les 5 premiers échantillons d'un RDD en niveau info précédé d'un message et de la longueur totable du RDD entre []
    """
    logging.info('%s [%d] :', message, rdd.count())
    logging.info(rdd.take(take))

def initDataDir(logfilename="./logfile.log"):
    """
    initialise les variables correspondant à l'arborescence DATA
    """
    # répertoire de base
    baseDir = os.path.dirname(__file__)
    print("baseDir: " + baseDir)

    # répertoire des données en entrée
    dataInDir = os.path.dirname(__file__) + "/DATA/IN"
    print("dataInDir: " + dataInDir)

    # répertoire des données en sortie
    dataOutDir = os.path.dirname(__file__) + "/DATA/OUT"
    print("dataOutDir: " + dataOutDir)

    # répertoire de log
    dataLogDir = os.path.dirname(__file__) + "/DATA/LOG"
    logFile = dataLogDir + logfilename
    print("logFile: " + logFile)

    return baseDir, dataInDir, dataOutDir, logFile

def initSparkContext():
    # configuration du spark context et de sa configuration
    conf = (SparkConf().setMaster("local").setAppName("My app").set("spark.executor.memory", "1g"))
    sc = SparkContext(conf=conf)

    return conf, sc

# ecriture du fichier résultat
# outFile = os.path.join(dataOutDir, "titanic.txt")
# print("outFile: " + outFile)

# on ecrit le RDD dans un fichier
# with open(outFile, "w") as testFile:
#     for item in mystring:
#         testFile.write("%s\n" % item)