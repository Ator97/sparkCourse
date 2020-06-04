from pyspark import SparkContext

sc = SparkContext(master="local", appName="App4")

'''
Trabajos, etapas y tareas
    Un programa Spark define un DAG conectando los diferentes RDDs
        Las transformaciones crean RDDs hijos a partir de RDDs padres
    Las acciones traducen el DAG en un plan de ejecución
        El driver envía un trabajo (job) para computar todos los RDDs implicados en la acción
        El job se descompone en una o más etapas (stages)
        Cada etapa está asociada a uno o más RDDs del DAG
        Las etapas se procesan en orden, lanzándose tareas (tasks) individuales que computan segmentos de los RDDs
    Pipelining: varios RDDs se pueden computan en una misma etapa si se verifica que:
        Los RDDs se pueden obtener de sus padres sin movimiento de datos (p.e. map), o bien
        si alguno de los RDDs se ha cacheado en memoria o disco
    En el interfaz web de Spark se muestran información sobre las etapas y tareas (más info: método toDebugString() de los RDDs)
'''
%spark
// A partir de los ficheros secuencia de apat63_99-seq obtener para cada país y año el número de patentes
import org.apache.hadoop.io.Text

val prdd = sc.sequenceFile("../datos/apat63_99-seq", classOf[Text], classOf[Text])
println("Número de particiones del RDD"+ prdd.getNumPartitions)

//Cada registro de apar63_99-seq tiene un par (país, patente,año)
val prdd2 = prdd.map(p => (p._1+"-"+p._2.toString().split(",")(1), 1) )
                .reduceByKey(_+_)
                
val s = prdd2.take(10)

println("\nInformación de depurado:")
println(prdd2.toDebugString)

'''

Acumuladores
Permiten agregar valores desde los worker nodes, que se pasan al driver
    Útiles para contar eventos
    Solo el driver puede acceder a su valor
    Acumuladores usados en transformaciones de RDDs pueden ser incorrectos
        Si el RDD se recalcula, el acumulador puede actualizarse
        En acciones, este problema no ocurre
    Por defecto, los acumuladores son enteros o flotantes
        Es posible crear “acumuladores a medida” usando AccumulatorParam
'''
%pyspark
from numpy.random import randint

npares = sc.accumulator(0)

def esPar(n):
    global npares
    if n%2 == 0:
        npares += 1

rdd = sc.parallelize(randint(100, size=10000))
rdd.foreach(esPar)

print("N pares: %d" % npares.value)

'''
Variables de broadcast
    Por defecto, todas las variables compartidas (no RDDs) son enviadas a todos los ejecutores
        Se reenvían en cada operación en la que aparezcan
    Variables de broadcast: permiten enviar de forma eficiente variables de solo lectura a los workers
        Se envían solo una vez
'''
%pyspark
from operator import add

# dicc es una variable de broadcast
dicc=sc.broadcast({"a":"alpha","b":"beta","c":"gamma"})

rdd=sc.parallelize([("a", 1),("b", 3),("a", -4),("c", 0)])
reduced_rdd = rdd.reduceByKey(add).map(lambda (x,y): (dicc.value[x],y))

print(reduced_rdd.collect())

'''
Trabajando a nivel de partición
Una operación map se hace para cada elemento de un RDD
    Puede implicar operaciones redundantes (p.e. abrir una conexión a una BD)
    Puede ser poco eficiente
Se pueden hacer map y foreach una vez por partición:
    Métodos mapPartitions(), mapPartitionsWithIndex() y foreachPartition()
'''
%pyspark
nums = sc.parallelize([1,2,3,4,5,6,7,8,9], 2)
print(nums.glom().collect())

def sumayCuenta(iterador):
    sumaCuenta = [0,0]
    for i in iterador:
        sumaCuenta[0] += i
        sumaCuenta[1] += 1
    return sumaCuenta
    
print(nums.mapPartitions(sumayCuenta).glom().collect())


%pyspark
def sumayCuentaIndex(index, it):
    return "Particion "+str(index), sumayCuenta(it)

print(nums.mapPartitionsWithIndex(sumayCuentaIndex).glom().collect())




%pyspark
import os
import tempfile

def f(iterator):
    tempfich, _ = tempfile.mkstemp(dir=tempdir)
    for x in iterator: 
        os.write(tempfich, str(x)+'\t')
    os.close(tempfich)
        
tempdir = "/tmp/foreachPartition"

if not os.path.exists(tempdir):
    os.mkdir(tempdir)
    nums.foreachPartition(f)


%sh
TEMP=/tmp/foreachPartition
echo "Ficheros creados"
ls -l $TEMP
echo
echo "Contenido de los ficheros"
for f in $TEMP/*;do cat $f; echo; echo "===="; done
rm -rf $TEMP





'''






Tarea

A partir del fichero apat63_99.txt, crear un conjunto de ficheros secuencia, que se almacenarán en el directorio apat63_99-seq. En estos ficheros, la clave tiene que ser el país (campo “COUNTRY”) y el valor un string formado por el número de patente (campo “PATENT”) y el año de concesión (campo “GYEAR”) separados por una coma. Una línea de esto ficheros será, por ejemplo:

    BE      3070801,1963

FINISHED
Took 0 sec. Last updated by anonymous at July 23 2017, 6:46:38 AM. (outdated)
READY
Tarea

Escribir un programa Scala Spark que, a partir de los ficheros cite75_99.txt y apat63_99-seq, obtenga, para cada patente, el país, el año y el número de citas.

Utilizar un full outer join para unir, por el campo común (el número de patente) los RDDs asociados a ambos ficheros.




Tarea

Desarrollar un script PySpark, que, a partir de los ficheros secuencia en apat63_99-seq cree un RDD clave valor, en el cual la clave es un país y el valor una lista de tuplas, en la que cada tupla esté formada por un año y el número de patentes de ese país en ese año. Además, debeis utilizar el contenido del fichero country_codes.txt (localizado en ../datos/country_codes.txt) como una variable de broadcast y substituir el código del país por su nombre. Por último, el RDD creado debe estar ordenado por el nombre del país y, para cada país, la lista de valores ordenados por año.

Recordad que cada registro de zapat63_99-seq tiene un par clave valor (país patente,año), siendo tanto la clave como el valor de tipo org.apache.hadoop.io.Text.
'''

