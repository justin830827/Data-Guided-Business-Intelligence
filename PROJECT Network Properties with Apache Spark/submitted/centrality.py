from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from pyspark.sql.functions import explode
import networkx as nx

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def closeness(g):
    # Get list of vertices. We'll generate all the shortest paths at
    # once using this list.
    vlist = g.vertices.select('id').flatMap(lambda x:x).collect()
    # first get all the path lengths.
    paths = g.shortestPaths(landmarks=vlist).select("id", explode("distances"))
    # Break up the map and group by ID for summing
    # Sum by ID
    df = paths.groupBy('id').sum()
    # Get the inverses and generate desired dataframe.
    nid = df.select('id').flatMap(lambda x:x).collect()
    close = df.select('sum(value)').flatMap(lambda x:x).collect()
    j=0
    cc = 0.0
    for i in close:
        cc = 1/float(i)
        close[j]= cc
        j=j+1
    df2 = sqlContext.createDataFrame(zip(nid,close),['id','closeness'])
    return df2


print("Reading in graph for problem 2.")

graph = sc.parallelize([('A','B'),('A','C'),('A','D'),
	('B','A'),('B','C'),('B','D'),('B','E'),
	('C','A'),('C','B'),('C','D'),('C','F'),('C','H'),
	('D','A'),('D','B'),('D','C'),('D','E'),('D','F'),('D','G'),
	('E','B'),('E','D'),('E','F'),('E','G'),
	('F','C'),('F','D'),('F','E'),('F','G'),('F','H'),
	('G','D'),('G','E'),('G','F'),
	('H','C'),('H','F'),('H','I'),
	('I','H'),('I','J'),
	('J','I')])
e = sqlContext.createDataFrame(graph,['src','dst'])
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()
print("Generating GraphFrame.")
g = GraphFrame(v,e)

print("Calculating closeness.")
closeness(g).sort('closeness',ascending=False).show()
closeness(g).sort('closeness',ascending=False).toPandas().to_csv("centrality_out.csv")
