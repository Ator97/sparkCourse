from pyspark import SparkContext

sc = SparkContext(master="local", appName="App3")


'''
RDDs con pares clave/valor (aka Pair RDDs)
    Tipos de datos muy usados en Big Data (MapReduce)
    Spark dispone de operaciones especiales para su manejo
'''
'''
A partir de una lista de tuplas
'''
%pyspark
prdd = sc.parallelize([('a',2), ('b',5), ('a',3)])
print(prdd.collect())

prdd = sc.parallelize(zip(['a', 'b', 'c'], range(3)))
print(prdd.collect())
'''
A partir de otro RDD
'''
%pyspark
# Ejemplo usando un fichero
# Para cada línea ontenemos una tupla, siendo el primer elemento
# la primera palabra de la línes, y el segundo la línea completa
linesrdd = sc.textFile("../datos/quijote.txt", use_unicode=False)
prdd = linesrdd.map(lambda x: (x.split(" ")[0], x))

print('Par (1ª palabra, línea): {0}\n'.format(prdd.takeSample(False, 1)))

%pyspark
# Usando keyBy(f): Crea tuplas de los elementos del RDD usando f para obtener la clave.
nrdd = sc.parallelize(xrange(2,5))
prdd = nrdd.keyBy(lambda x: x*x)

print(prdd.collect())

%pyspark
# zipWithIndex(): Zipea el RDD con los índices de sus elementos.
rdd = sc.parallelize(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], 3)
prdd = rdd.zipWithIndex()
print(rdd.glom().collect())

print(prdd.collect())

# Este método dispara un Spark job cuando el RDD tiene más de una partición.

%pyspark
# zipWithUniqueId(): Zipea el RDD con identificadores únicos (long) para cada elemento.
# Los elementos en la partición k-ésima obtienen los ids k, n+k, 2*n+k,... siendo n = nº de particiones
# No dispara un trabajo Spark
rdd = sc.parallelize(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'], 3)
print("Particionado del RDD: {0}".format(rdd.glom().collect()))
prdd = rdd.zipWithUniqueId()

print(prdd.collect())


'''
    Mediante un zip de dos RDDs
        Los RDDs deben tener el mismo número de particiones y el mismo número de elementos en cada partición
'''
%pyspark
rdd1 = sc.parallelize(xrange(0, 5), 2)
rdd2 = sc.parallelize(range(1000, 1005), 2)
prdd = rdd1.zip(rdd2)

print(prdd.collect())
'''
Transformaciones sobre un único RDD clave/valor
Sobre un único RDD clave/valor podemos efectuar transformaciones de agregación a nivel de clave y transformaciones que afectan a las claves o a los valores

Transformaciones de agregación
'''
'''
    reduceByKey(func)/foldByKey(func)
        Devuelven un RDD, agrupando los valores asociados a la misma clave mediante func
        Similares a reduce y fold sobre RDDs simples
'''
%pyspark
from operator import add
prdd   = sc.parallelize([('a', 2), ('b', 5), ('a', 8), ('b', 6), ('b', 2)]).cache()
redrdd = prdd.reduceByKey(add)

print(redrdd.collect())
'''
    groupByKey() agrupa valores asociados a misma clave
        Operación muy costosa en comunicaciones
        Mejor usar operaciones de reducción
'''
%pyspark
grouprdd = prdd.groupByKey()

print(grouprdd.collect())
print

lista = [(k, list(v)) for k, v in grouprdd.collect()]
print(lista)
'''
    combineByKey(createCombiner(func1), mergeValue(func2), mergeCombiners(func3))
        Método general para agregación por clave, similar a aggregate
        Especifica tres funciones:
        createCombiner al recorrer los elementos de cada partición, si nos encontramos una clave nueva se crea un acumulador y se inicializa con func1
        mergeValue mezcla los valores de cada clave en cada partición usando func2
        mergeCombiners mezcla los resultados de las diferentes particiones mediante func3
    Los valores del RDD de salida pueden tener un tipo diferente al de los valores del RDD de entrada.
'''
%pyspark
# Para cada clave, obten una tupla que tenga la suma y el número de valores
sumCount = prdd.combineByKey(
                            (lambda x: (x, 1)),
                            (lambda x, y: (x[0]+y, x[1]+1)),
                            (lambda x, y: (x[0]+y[0], x[1]+y[1])))

print(sumCount.collect())

# Con el RDD anterior, obtenemos la media de los valores
m = sumCount.mapValues(lambda v: float(v[0])/v[1])
print(m.collect())
'''

Transformaciones sobre claves o valores

    keys() devuelve un RDD con las claves
    values() devuelve un RDD con los valores
    sortByKey() devuelve un RDD clave/valor con las claves ordenadas
'''
%pyspark
print("RDD completo: {0:>46s}".format(prdd.collect()))
print("RDD con las claves: {0:>25s}".format(prdd.keys().collect()))
print("RDD con los valores: {0:>18}".format(prdd.values().collect()))
print("RDD con las claves ordenadas: {0}".format(prdd.sortByKey().collect()))
'''
    mapValues(func) devuelve un RDD aplicando una función sobre los valores
    flatMapValues(func) devuelve un RDD aplicando una función sobre los valores y “aplanando” la salida
'''
%pyspark
mapv = prdd.mapValues(lambda x: (x, 10*x))
print(mapv.collect())

fmapv = prdd.flatMapValues(lambda x: (x, 10*x))
print(fmapv.collect()
'''
Transformaciones sobre dos RDDs clave/valor
Combinan dos RDDs de tipo clave/valor para obtener un tercer RDD.
'''
'''
join/leftOuterJoin/rightOuterJoin/fullOuterJoin realizan inner/outer/full joins entre los dos RDDs
'''
%pyspark
rdd1 = sc.parallelize([("a", 2), ("b", 5), ("a", 8)]).cache()
rdd2 = sc.parallelize([("c", 7), ("a", 1)]).cache()

rdd3 = rdd1.join(rdd2)

print(rdd3.collect())


%pyspark
rdd3 = rdd1.leftOuterJoin(rdd2)
print(rdd3.collect())

%pyspark
rdd3 = rdd1.rightOuterJoin(rdd2)
print(rdd3.collect())

%pyspark
rdd3 = rdd1.fullOuterJoin(rdd2)
print(rdd3.collect())

'''
    subtractByKey elimina elementos con una clave presente en otro RDD
'''
%pyspark
rdd3 = rdd1.subtractByKey(rdd2)

print(rdd3.collect())

'''
cogroup agrupa los datos que comparten la misma clave en ambos RDDs
'''
%pyspark
rdd3 = rdd1.cogroup(rdd2)

print(rdd3.collect())

map = rdd3.mapValues(lambda v: [list(l) for l in v]).collectAsMap()

print(map)

'''
Acciones sobre RDDs clave/valor
Sobre los RDDs clave/valor podemos aplicar las acciones para RDDs simples y algunas adicionales.
'''
'''
collectAsMap() obtiene el RDD en forma de mapa
'''
%pyspark
prdd = sc.parallelize([("a", 7), ("b", 5), ("a", 8)]).cache()

rddMap = prdd.collectAsMap()

print(rddMap)
'''
countByKey() devuelve un mapa indicando el número de ocurrencias de cada clave
'''
%pyspark
countMap = prdd.countByKey()

print(countMap)

'''
    lookup(key) devuelve una lista con los valores asociados con una clave
'''
%pyspark
listA = prdd.lookup('a')

print(listA)