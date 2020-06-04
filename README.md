# Spark Course

## Ejecutar

Es necesario tener instalado vagrant y virtualbox.

```console
username@hostname:~$ vagrant up
```

Inicializará vagrant e instalará los paquetes necesarios para poder ejecutar Spark2 con Python3.


Una vez instalado, es necesario conectarse por ssh para levantar jupyter notebook. Primero requermos la ip publica, por lo que con el siguiente comando conocemos la ip:


```console
username@hostname:~$ ssh -c "hostname -I | cut -d' ' -f2" 2>/dev/null
```

Una vez obtenida, ejecutamos:

```console
username@hostname:~$ ssh -L 8888:localhost:8888 vagrant@ipObtenida
vagrant@ipObtenida's password: 

vagrant@spark:~$ jupyter-notebook --no-browser
[I 23:42:05.839 NotebookApp] JupyterLab extension loaded from /home/vagrant/anaconda3/lib/python3.7/site-packages/jupyterlab
[I 23:42:05.839 NotebookApp] JupyterLab application directory is /home/vagrant/anaconda3/share/jupyter/lab
[I 23:42:05.842 NotebookApp] Serving notebooks from local directory: /home/vagrant
[I 23:42:05.842 NotebookApp] The Jupyter Notebook is running at:
[I 23:42:05.842 NotebookApp] http://localhost:8888/?token=5b1546200a688a72d662b7475d69e9a3effaaf653b5ab4fd
[I 23:42:05.842 NotebookApp]  or http://127.0.0.1:8888/?token=5b1546200a688a72d662b7475d69e9a3effaaf653b5ab4fd
[I 23:42:05.842 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).

```

Y Finalmente daremos doble click sobre cualquiera de las ligas que nos expulse jupyter-notebook --no-browser.
No abrirá una ventana en nuestro navegador por defecto una pestaña de juputer anaconda listo para trabajar.


## Notas sobre Vagrant

Una vez descargada la máquina virtual, podemos interactuar con ella conectandonos con ayuda de ssh. Aun que poemos conectarnos directamente a la máquina virutal, esta  se encuentra ejecutandose en segundo plano. Es altamente recomendable que contine así ya que vagrant posee lo necesario para un despliege completo. 

Con el siguiente ejemplo podemos conectarno a la maquina virutal:

```console
username@hostname:~$ vagrant ssh
```
Si deseamos terminar la máquna virutal bastará con :

```console
username@hostname:~$ vagrant halt
```

Por otro lado si ya no deseamos mantenerla  en nuestro disco duro podemos eliminarla con:

```console
username@hostname:~$ vagrant destroy
```

Es importante mencionar, si es la primera vez que usar vagrant, el proceso de descarga y configuacion y ejecución es el mismo, por lo que con 'vagrant up' podemos hacer ambas tareas.


## Temario 


1. Presentation
	1. Presentación de curso
	2. Instalación de ambiente virtual	
2. Introduction to Spark
	1. Introducción a Apache Spark
	2. Introducción a los RDDs
	3. Introducción a los DataFrames
3. Operaciones RDDs
	1. Transformaciones y acciones
	2. Transformaciones sobre un RDD
	3. Transformaciones sobgre dos RDD
	4. Acciones de Agregación en un RDDs I
	5. Acciones de Agregacion en un RDDs II
	6. Otros tipos de acciones sobre RDDs
4. Tipos avanzados de RDDs
	1. RDDs con clave-valor
	2. Transformaciones sobre RDDs con clave-valor I
	3. Transformaciones sobre RDDs con clave-valor II
	4. Transformaciones sobre RDDs con clave-valor III
	5. Acciones sobre RDDs con clave-valor III
5. Persistencia y particionado
	1. Lectura y escritura de ficheros
	2. Presistencia y particionado de datos
6. SparkSQL
	1. Creación de DataFrames
	2. Transformaciones con Dataframes
	3. Acciones con DataFrames
