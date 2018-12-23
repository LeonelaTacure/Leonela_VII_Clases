#Subo mi archivo al DataLake
hdfs dfs -ls /
hdfs dfs -ls /DataLake
hdfs dfs -mkdir /DataLake/Landing
hdfs dfs -ls /DataLake/Landing
hdfs dfs -put quijote.txt /DataLake/Landing

from pyspark.sql import 
SparkSession
warehouseLocation = "/DataLake/Landing"

spark = SparkSession\

.builder.appName("TestSparkCTICLAB1")\

.config("spark.sql.warehouse.dir", warehouseLocation)\

.enableHiveSupport()\

.getOrCreate()

read = spark.sparkContext.textFile("/DataLake/Landing/quijote.txt",4) #Creo mi archivo
read.take(10)							      #Verifico que el archivo halla subido
read.getNumPartitions()						      #verifico particiones

#Cuento la cantidad de palabras
rd = read.flatMap(lambda line: line.split())
rd.count()

#Cambio las particiones de 4 a 2
read1= read.coalesce(2)
read1.getNumPartitions()

#Poner en cache el RDD
read1.cache()

#Generar el RDD de valores [1,5,8,6,30,-45,45,150,0] y quedarse con los elementos pares
data1 = [1,5,8,6,30,-45,45,150,0]
rdd1 = sc.parallelize(data1)
rdds = rdd1.filter(lambda x:x%2==0)
rdds.collect()

#Generar el RDD de valores [("maths",52), ("English",75), ("science",82), ("computer",65), ("maths",85)] y ordenarlos de manera descendente teniendo los valores en mayúscula
data2 = [("maths",52), ("English",75), ("science",82), ("computer",65), ("maths",85)] 
rdds2 = sc.parallelize(data2)
rdds2.sortByKey(ascending = False, keyfunc = lambda i: i.upper()).collect()
