# Guide Pyspark sur IntelliJ IDEA
[pyspark-sur-votre-ide-comment-faire](http://blog.xebia.fr/2016/06/20/pyspark-sur-votre-ide-comment-faire/)

## Ajouter les paths de pyspark dans l'IDE

 Dans `File->Project Structure->SDKs->son environnement conda`, ajouter:

* `/opt/spark/spark-2.0.2-bin-hadoop2.7/python`
* `/opt/spark/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip`

## Positionner les variables d'environnement

* `PYTHONPATH` `/opt/spark/spark-2.0.2-bin-hadoop2.7/python:/opt/spark/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip`
* `SPARK_HOME` `/opt/spark/spark-2.0.2-bin-hadoop2.7`

# Lancer un script python en ligne de commande
```
$ spark-submit titanic.py
```