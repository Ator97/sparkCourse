#ssh -N -f -L localhost:8888:localhost:8888 spark@192.168.11.114 
from pyspark import SparkContext

sc = SparkContext(master="local", appName="App1")

'''
RDD: Resilient Distributed Datasets
    Colección inmutable y distribuida de elementos que pueden manipularse en paralelo
    Un programa Spark opera sobre RDDs:
        Creación de RDDs
        Transformación de RDDs (map, filter, etc.)
        Realización de acciones sobre RDDs para obtener resultados
    Spark automáticamente distribuye los datos y paraleliza las operaciones
'''
%pyspark
# Ejemplo en PySpark
rdd1 = sc.parallelize([1,2,3])
import numpy as np
rdd2=sc.parallelize(np.array(range(100)))
print(rdd2.glom().collect())
'''
Leyendo datos de un fichero
'''
%pyspark
# Ejemplo en PySpark (fichero ../datos/quijote.txt)
quijote = sc.textFile("../datos/quijote.txt")
print(quijote.take(1000))
'''
Particiones
Spark divide el RDD en en conjunto de particiones
    El número de particiones por defecto es función del tamaño del
    cluster o del número de bloques del fichero (p.e. bloques HDFS)
    Se puede especificar otro valor en el momento de crear el RDD
'''
%pyspark
rdd = sc.parallelize([1,2,3,4], 2)
print(rdd.glom().collect())
print(rdd.getNumPartitions())
'''
Transformaciones
Operaciones sobre RDDs que devuelven un nuevo RDD
    Se computan de forma “perezosa” ( lazy )
    Normalmente, ejecutan una función (anónima o no) sobre cada uno de
    los elementos del RDD de origen
'''
%pyspark
quijs = quijote.filter(lambda l: "Quijote" in l)
sanchs = quijote.filter(lambda l: "Sancho" in l)
quijssancs = quijs.intersection(sanchs)
quijssancs.cache()
'''
Acciones
Obtienen datos de salida a partir de los RDDs
    Devuelven valores al driver o al sistema de almacenamiento
    Fuerzan a que se realicen las transformaciones pendientes
'''
%pyspark
nqs = quijssancs.count()
print("Líneas con Quijote y Sancho {0}".format(nqs))
for l in quijssancs.takeSample(False,10):
    print(l)
