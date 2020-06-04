from pyspark import SparkContext

sc = SparkContext(master="local", appName="App4")

'''
Persistencia y particionado
'''
'''
Persistencia

Problema al usar un RDD varias veces:

    Spark recomputa el RDD y sus dependencias cada vez que se ejecuta una acción
    Muy costoso (especialmente en problemas iterativos)

Solución

    Conservar el RDD en memoria y/o disco
    Métodos cache() o persist()

Niveles de persistencia (definidos en pyspark.StorageLevel)
Nivel 	Espacio 	CPU 	Memoria/Disco 	Descripción
MEMORY_ONLY 	Alto 	Bajo 	Memoria 	Guarda el RDD como un objeto Java no serializado en la JVM. Si el RDD no cabe en memoria, algunas particiones no se cachearán y serán recomputadas “al vuelo” cada vez que se necesiten. Nivel por defecto en Java y Scala.
MEMORY_ONLY_SER 	Bajo 	Alto 	Memoria 	Guarda el RDD como un objeto Java serializado (un byte array por partición). Nivel por defecto en Python, usando pickle.
MEMORY_AND_DISK 	Alto 	Medio 	Ambos 	Guarda el RDD como un objeto Java no serializado en la JVM. Si el RDD no cabe en memoria, las particiones que no quepan se guardan en disco y se leen del mismo cada vez que se necesiten
MEMORY_AND_DISK_SER 	Bajo 	Alto 	Ambos 	Similar a MEMORY_AND_DISK pero usando objetos serializados.
DISK_ONLY 	Bajo 	Alto 	Disco 	Guarda las particiones del RDD solo en disco.
OFF_HEAP 	Bajo 	Alto 	Memoria 	Guarda el RDD serializado usando memoria off-heap (fuera del heap de la JVM) lo que puede reducir el overhead del recolector de basura
Nivel de persistencia

    En Scala y Java, el nivel por defecto es MEMORY_ONLY

    En Python, los datos siempre se serializan (por defecto como objetos pickled)
        Los niveles MEMORY_ONLY, MEMORY_AND_DISK son equivalentes a MEMORY_ONLY_SER, MEMORY_AND_DISK_SER
        Es posible especificar serialización marshal al crear el SparkContext
sc = SparkContext(master="local", appName="Mi app", serializer=pyspark.MarshalSerializer())

Recuperación de fallos

    Si falla un nodo con datos almacenados, el RDD se recomputa

        Añadiendo _2 al nivel de persistencia, se guardan 2 copias del RDD

Gestión de la cache

    Algoritmo LRU para gestionar la cache
        Para niveles solo memoria, los RDDs viejos se eliminan y se recalculan
        Para niveles memoria y disco, las particiones que no caben se escriben a disco

'''


%pyspark

rdd = sc.parallelize(range(1000), 10)

print(rdd.is_cached)


%pyspark

rdd.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

print(rdd.is_cached)

print("Nivel de persistencia de rdd: {0} ".format(rdd.getStorageLevel()))


%pyspark

rdd2 = rdd.map(lambda x: x*x)
print(rdd2.is_cached)


%pyspark

rdd2.cache() # Nivel por defecto
print(rdd2.is_cached)
print("Nivel de persistencia de rdd2: {0}".format(rdd2.getStorageLevel()))

%pyspark

rdd2.unpersist() # Sacamos rdd2 de la cache
print(rdd2.is_cached)

'''
Particionado
El número de particiones es función del tamaño del cluster o el número de bloques del fichero en HDFS
    Es posible ajustarlo al crear u operar sobre un RDD
    El paralelismo de RDDs que derivan de otros depende del de sus RDDs padre
    Dos funciones útiles:
        rdd.getNumPartitions() devuelve el número de particiones del RDD
        rdd.glom() devuelve un nuevo RDD juntando los elementos de cada partición en una lista
'''
%pyspark
rdd = sc.parallelize([1, 2, 3, 4, 2, 4, 1], 4)
pairs = rdd.map(lambda x: (x, x))

print("RDD pairs = {0}".format(
        pairs.collect()))
print("Particionado de pairs: {0}".format(
        pairs.glom().collect()))
print("Número de particiones de pairs = {0}".format(
        pairs.getNumPartitions()))



%pyspark
# Reducción manteniendo el número de particiones
print("Reducción con 4 particiones: {0}".format(
        pairs.reduceByKey(lambda x, y: x+y).glom().collect()))

%pyspark
# Reducción modificando el número de particiones
print("Reducción con 2 particiones: {0}".format(
       pairs.reduceByKey(lambda x, y: x+y, 2).glom().collect()))



'''
Funciones de reparticionado

    repartition(n) devuelve un nuevo RDD que tiene exactamente n particiones
    coalesce(n) más eficiente que repartition, minimiza el movimiento de datos
        Solo permite reducir el número de particiones
    partitionBy(n,[partitionFunc]) Particiona por clave, usando una función de particionado (por defecto, un hash de la clave)
        Solo para RDDs clave/valor
        Asegura que los pares con la misma clave vayan a la misma partición

'''

%pyspark
pairs5 = pairs.repartition(5)
print("pairs5 con {0} particiones: {1}".format(
        pairs5.getNumPartitions(),
        pairs5.glom().collect()))


 %pyspark
pairs2 = pairs5.coalesce(2)
print("pairs2 con {0} particiones: {1}".format(
        pairs2.getNumPartitions(),
        pairs2.glom().collect()))


        %pyspark
pairs_clave = pairs2.partitionBy(3)
print("Particionado por clave ({0} particiones): {1}".format(
        pairs_clave.getNumPartitions(),
        pairs_clave.glom().collect())) 




'''
Lectura y escritura de ficheros
'''

'''
Sistemas de ficheros soportados
    Igual que Hadoop, Spark soporta diferentes filesystems: local, HDFS, Amazon S3
        En general, soporta cualquier fuente de datos que se pueda leer con Hadoop
    También, acceso a bases de datos relacionales o noSQL
        MySQL, Postgres, etc. mediante JDBC
        Apache Hive, HBase, Cassandra o Elasticsearch
Formatos de fichero soportados
    Spark puede acceder a diferentes tipos de ficheros:
        Texto plano, CSV, ficheros sequence, JSON, protocol buffers y object files
            Soporta ficheros comprimidos
        Veremos el acceso a algunos tipos en esta clase, y dejaremos otros para más adelante
'''
'''

Ejemplos con ficheros de texto

En el directorio ../datos/libros hay un conjunto de ficheros de texto comprimidos.

'''
%sh
# Ficheros de entrada
ls ../datos/libros
'''
Funciones de lectura y escritura con ficheros de texto
    sc.textFile(nombrefichero/directorio) Crea un RDD a partir las líneas de uno o varios ficheros de texto
        Si se especifica un directorio, se leen todos los ficheros del mismo, creando una partición por fichero
        Los ficheros pueden estar comprimidos, en diferentes formatos (gz, bz2,…)
        Pueden especificarse comodines en los nombres de los ficheros
    sc.wholeTextFiles(nombrefichero/directorio) Lee ficheros y devuelve un RDD clave/valor
        clave: path completo al fichero
        valor: el texto completo del fichero
    rdd.saveAsTextFile(directorio_salida) Almacena el RDD en formato texto en el directorio indicado
        Crea un fichero por partición del rdd
'''
%pyspark
# Lee todos los ficheros del directorio
# y crea un RDD con las líneas
lineas = sc.textFile("../datos/libros")

# Se crea una partición por fichero de entrada
print("Número de particiones del RDD lineas = {0}".format(
       lineas.getNumPartitions()))

%pyspark
# Obtén las palabras usando el método split (split usa un espacio como delimitador por defecto)
palabras = lineas.flatMap(lambda x: x.split())
print("Número de particiones del RDD palabras = {0}".format(
       palabras.getNumPartitions()))

%pyspark
# Obtén las palabras usando el método split (split usa un espacio como delimitador por defecto)
palabras = lineas.flatMap(lambda x: x.split())
print("Número de particiones del RDD palabras = {0}".format(
       palabras.getNumPartitions()))


%pyspark
# Toma una muestra aleatoria de palabras
print(palabras2.takeSample(False, 10))


%pyspark
# Salva el RDD palabras en varios ficheros de salida
# (un fichero por partición)
palabras2.saveAsTextFile("file:///tmp/salidatxt")

%sh
# Ficheros de salida
ls -l /tmp/salidatxt
head /tmp/salidatxt/part-00002



%pyspark
# Lee los ficheros y devuelve un RDD clave/valor
# clave->nombre fichero, valor->fichero completo
prdd = sc.wholeTextFiles("../datos/libros/p*.gz")
print("Número de particiones del RDD prdd = {0}\n".format(
       prdd.getNumPartitions()))


%pyspark
# Obtiene un lista clave/valor
# clave->nombre fichero, valor->numero de palabras
lista = prdd.mapValues(lambda x: len(x.split())).collect()

for libro in lista:
    print("El fichero {0:14s} tiene {1:6d} palabras".format(
           libro[0].split("/")[-1], libro[1]))

'''
Ficheros Sequence
Ficheros clave/valor usados en Hadoop
    Sus elementos implementan la interfaz Writable
'''

%pyspark
rdd = sc.parallelize([("a",2), ("b",5), ("a",8)], 2)

# Salvamos el RDD clave valor como fichero de secuencias
rdd.saveAsSequenceFile("file:///tmp/sequenceoutdir2")

%sh
echo 'Directorio de salida'
ls -l /tmp/sequenceoutdir2
echo 'Intenta leer uno de los fichero'
cat /tmp/sequenceoutdir2/part-00000
echo
echo  'Lee el fichero usando Hadoop'
/opt/hadoop/bin/hdfs dfs -text /tmp/sequenceoutdir2/part-00001

%pyspark
# Lo leemos en otro RDD
rdd2 = sc.sequenceFile("file:///tmp/sequenceoutdir2", 
                       "org.apache.hadoop.io.Text", 
                       "org.apache.hadoop.io.IntWritable")
                       
print("Contenido del RDD {0}".format(rdd2.collect()))



'''

Formatos de entrada/salida de Hadoop
Spark puede interactuar con cualquier formato de fichero soportado por Hadoop
- Soporta las APIs “vieja” y “nueva”
- Permite acceder a otros tipos de almacenamiento (no fichero), p.e. HBase o MongoDB, a través de saveAsHadoopDataSet y/o saveAsNewAPIHadoopDataSet
'''

%pyspark
# Salvamos el RDD clave/valor como fichero de texto Hadoop (TextOutputFormat)
rdd.saveAsNewAPIHadoopFile("file:///tmp/hadoopfileoutdir", 
                            "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat",
                            "org.apache.hadoop.io.Text",
                            "org.apache.hadoop.io.IntWritable")

%sh
echo 'Directorio de salida'
ls -l /tmp/hadoopfileoutdir
cat /tmp/hadoopfileoutdir/part-r-00001

%pyspark
# Lo leemos como fichero clave-valor Hadoop (KeyValueTextInputFormat)
rdd3 = sc.newAPIHadoopFile("file:///tmp/hadoopfileoutdir", 
                          "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat",
                          "org.apache.hadoop.io.Text",
                          "org.apache.hadoop.io.IntWritable")
                          
print("Contenido del RDD {0}".format(rdd3.collect()))

