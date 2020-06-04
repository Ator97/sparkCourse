from pyspark import SparkContext

sc = SparkContext(master="local", appName="App2")

'''
Transformaciones elemento-a-elemento
Generan un nuevo RDD a partir de uno dado
'''
'''
`filter(func)` filtra los elementos de un RDD
'''
# Obtener los valores positivos de un rango de números
from __future__ import print_function
from test_helper import Test
rdd = sc.parallelize(xrange(-5,5))          # Rango [-5, 5)
filtered_rdd = rdd.filter(lambda x: x >= 0)   # Devuelve los positivos
'''
`map(func)` aplica una función a los elementos de un RDD
'''
# Ejemplo en PySpark
# Añade 1 a cada elemento del RDD
# Para cada elemento, obtiene una tupla (x, x**2)
def add1(x):
    return(x+1)

squared_rdd = (filtered_rdd
               .map(add1)                 # Añade 1 a cada elemento del RDD
               .map(lambda x: (x, x*x)))  # Para cada elemento, obtén una tupla (x, x**2)

Test.assertEquals(squared_rdd.collect(), [(1, 1), (2, 4), (3, 9), (4, 16), (5, 25)])
'''
flatMap(func) igual que map, pero “aplana” la salida
'''
%pyspark
squaredflat_rdd = (filtered_rdd
                   .map(add1)
                   .flatMap(lambda x: (x, x*x)))  # Da la salida en forma de lista
                   
Test.assertEquals(squaredflat_rdd.collect(), [1, 1, 2, 4, 3, 9, 4, 16, 5, 25])
'''
    sample(withReplacement, fraction, seed=None) devuelve una muestra del RDD
        withReplacement - si True, cada elemento puede aparecer varias veces en la muestra
        fraction - tamaño esperado de la muestra como una fracción del tamaño del RDD
            sin reemplazo: probabilidad de seleccionar un elemento, su valor debe ser [0, 1]
            con reemplazo: número esperado de veces que se escoge un elemento, su valor debe ser >= 0
        seed - semilla para el generador de números aleatorios
'''
%pyspark
srdd1 = squaredflat_rdd.sample(False, 0.5)
srdd2 = squaredflat_rdd.sample(True, 2)
srdd3 = squaredflat_rdd.sample(False, 0.8, 14)
print('s1={0}\ns2={1}\ns3={2}'.format(srdd1.collect(), srdd2.collect(), srdd3.collect()))
'''
    distinct() devuelve un nuevo RDD sin duplicados
        El orden de la salida no está definido
'''
distinct_rdd = squaredflat_rdd.distinct()
print(distinct_rdd.collect())
'''
groupBy(func) devuelve un RDD con los datos agrupados en formato clave/valor,
usando una función para obtener la clave
'''
grouped_rdd = distinct_rdd.groupBy(lambda x: x%3)
print(grouped_rdd.collect())
print([(x,sorted(y)) for (x,y) in grouped_rdd.collect()])
'''
Transformaciones sobre dos RDDs
Operaciones tipo conjunto sobre dos RDDs
'''
'''
rdda.union(rddb) devuelve un RDD con los datos de los dos de partida
'''
rdda = sc.parallelize(['a', 'b', 'c'])
rddb = sc.parallelize(['c', 'd', 'e'])
rddu = rdda.union(rddb)
Test.assertEquals(rddu.collect(),['a', 'b', 'c', 'c', 'd', 'e'])
'''
rdda.intersection(rddb) devuelve un RDD con los datos comunes en ambos RDDs
'''
%pyspark
rddi = rdda.intersection(rddb)
Test.assertEquals(rddi.collect(),['c'])
'''
rdda.subtract(rddb) devuelve un RDD con los datos del primer RDD menos los del segundo
'''
%pyspark
rdds = rdda.subtract(rddb)
Test.assertEquals(rdds.collect(), ['a', 'b'])
'''
rdda.cartesian(rddb) producto cartesiano de ambos RDDs (operación muy costosa)
'''
%pyspark
rddc = rdda.cartesian(rddb)
Test.assertEquals(rddc.collect(), 
                  [('a','c'),('a','d'),('a','e'),('b','c'),('b','d'),('b','e'),('c','c'), ('c','d'), ('c','e')])
'''
Acciones sobre RDDs simples
Obtienen datos (simples o compuestos) a partir de un RDD
Principales acciones de agregación: reduce y fold
'''
'''
    reduce(op) combina los elementos de un RDD en paralelo, aplicando un operador
        El operador de reducción debe ser un monoide conmutativo (operador binario asociativo y conmutativo)
        Primero se realiza la redución a nivel de partición y luego se van reduciendo los valores intermedios
'''
%pyspark
rdd = sc.parallelize(xrange(1,10), 8)  # rango [1, 10)
print(rdd.glom().collect())

# Reducción con una función lambda
p = rdd.reduce(lambda x,y: x*y) # r = 1*2*3*4*5*6*7*8*9 = 362880
print("1*2*3*4*5*6*7*8*9 = {0}".format(p))

# Reducción con un operador predefinido
from operator import add
s = rdd.reduce(add) # s = 1+2+3+4+5+6+7+8+9 = 45
print("1+2+3+4+5+6+7+8+9 = {0}".format(s))

# Prueba con un operador no conmutativo
p = rdd.reduce(lambda x,y: x-y) # r = 1-2-3-4-5-6-7-8-9 = -43
print("1-2-3-4-5-6-7-8-9 = {0}".format(p))

# No funciona con RDDs vacíos
#sc.parallelize([]).reduce(add)

'''
    fold(cero, op) versión general de reduce:
        Debemos proporcionar un valor inicial cero para el operador
        El valor inicial debe ser el valor identidad para el operador (p.e. 0 para suma; 1 para producto, o una lista vacía para concatenación de listas)
            Permite utilizar RDDs vacíos
        La función op debe ser un monoide conmutativo para garantizar un resultado consistente
            Comportamiento diferente a las operaciones fold de lenguajes como Scala
            El operador se aplica a nivel de partición (usando cero como valor inicial), y finalmente entre todas las particiones (usando cerode nuevo)
            Para operadores no conmutativos el resultado podría ser diferente del obtenido mediante un fold secuencial
'''
%pyspark
rdd = sc.parallelize([range(1,5), range(-10,-3), ['a', 'b', 'c']])
print(rdd.glom().collect())

f = rdd.fold([], lambda x,y: x+y)
print(f)

# Se puede hacer un fold de un RDD vacío
sc.parallelize([]).fold(0, add)
'''
Otras acciones de agregación: aggregate
'''
'''
    aggregate(cero,seqOp,combOp): Devuelve una colección agregando los elementos del RDD usando dos funciones:
        seqOp - agregación a nivel de partición: se crea un acumulador por partición (inicializado a cero) y se agregan los valores de la partición en el acumulador
        combOp - agregación entre particiones: se agregan los acumuladores de todas las particiones
        Ambas agregaciones usan un valor inicial cero (similar al caso de fold).
    Versión general de reduce y fold
    La primera función (seqOp) puede devolver un tipo, U, diferente del tipo T de los elementos del RDD
        seqOp agregar datos de tipo T y devuelve un tipo U
        combOp agrega datos de tipo U
        cero debe ser de tipo U
    Permite devolver un tipo diferente al de los elementos del RDD de entrada.
'''
%pyspark
l = [1, 2, 3, 4, 5, 6, 7, 8]
rdd = sc.parallelize(l)

# acc es una tupla de tres elementos (List, Double, Int)
# En el primer elemento de acc (lista) le concatenamos los elementos del RDD al cuadrado
# en el segundo, acumulamos los elementos del RDD usando multiplicación
# y en el tercero, contamos los elementos del RDD
seqOp  = (lambda acc, val: (acc[0]+[val*val], 
                            acc[1]*val, 
                            acc[2]+1))
# Para cada partición se genera una tupla tipo acc
# En esta operación se combinan los tres elementos de las tuplas
combOp = (lambda acc1, acc2: (acc1[0]+acc2[0], 
                              acc1[1]*acc2[1], 
                              acc1[2]+acc2[2]))
                              
a = rdd.aggregate(([], 1., 0), seqOp, combOp) 

print(a)

Test.assertEquals(a[1], 8.*7.*6.*5.*4.*3.*2.*1.)
Test.assertEquals(a[2], len(l))
'''
Acciones para contar elementos
    count() devuelve un entero con el número exacto de elementos del RDD
    countApprox(timeout, confidence=0.95) versión aproximada de count() que devuelve un resultado potencialmente incompleto en un tiempo máximo, incluso si no todas las tareas han finalizado. (Experimental).
        timeout es un entero largo e indica el tiempo en milisegundos
        confidence probabilidad de obtener el valor real. Si confidence es 0.90 quiere decir que si se ejecuta múltiples veces, se espera que el 90% de ellas se obtenga el valor correcto. Valor [0,1]
    countApproxDistinct(relativeSD=0.05) devuelve una estimación del número de elementos diferentes del RDD. (Experimental).
        relativeSD – exactitud relativa (valores más pequeños implican menor error, pero requieren más memoria; debe ser mayor que 0.000017).
'''
%pyspark
rdd = sc.parallelize([i % 20 for i in xrange(10000)], 16)
print("Número total de elementos: {0}".format(rdd.count()))
print("Número de elementos distintos: {0}".format(rdd.distinct().count()))

print("Número total de elementos (aprox.): {0}".format(rdd.countApprox(1, 0.4)))
print("Número de elementos distintos (approx.): {0}".format(rdd.countApproxDistinct(0.5)))
'''
    countByValue() devuelve el número de apariciones de cada elemento del RDD como un mapa (o diccionario) de tipo clave/valor
        Las claves son los elementos del RDD y cada valor, el número de ocurrencias de la clave asociada al mismo
'''
%pyspark
rdd = sc.parallelize(list("abracadabra")).cache()
mimapa = rdd.countByValue()

print(type(mimapa))
print(mimapa.items())
'''
Acciones para obtener valores
    Estos métodos deben usarse con cuidado, si el resultado esperado es muy grande puede saturar la memoria del driver
'''
'''
collect() devuelve una lista con todos los elementos del RDD
'''
%pyspark
lista = rdd.collect()
print(lista)
'''
    take(n) devuelve los n primeros elementos del RDD
    takeSample(withRep, n, [seed]) devuelve n elementos aleatorios del RDD
        withRep: si True, en la muestra puede aparecer el mismo elemento varias veces
        seed: semilla para el generador de números aleatorios
'''
%pyspark
t = rdd.take(4)
print(t)
s = rdd.takeSample(False, 4)
print(s)
'''
    top(n) devuelve una lista con los primeros n elementos del RDD ordenados en orden descendente
    takeOrdered(n,[orden]) devuelve una lista con los primeros n elementos del RDD en orden ascendente (opuesto a top), o siguiendo el orden indicado en la función opcional
'''
%pyspark
rdd = sc.parallelize([8, 4, 2, 9, 3, 1, 10, 5, 6, 7]).cache()

print("4 elementos más grandes: {0}".format(rdd.top(4)))

print("4 elementos más pequeños: {0}".format(rdd.takeOrdered(4)))

print("4 elementos más grandes: {0}".format(rdd.takeOrdered(4, lambda x: -x)))