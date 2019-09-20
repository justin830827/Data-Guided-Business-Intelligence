import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

def articulations(g, usegraphframe=False):
	# Get the starting count of connected components
	elist = g.edges.select('src','dst').map(lambda x: (x[0],x[1])).collect()
	vlist = g.vertices.map(lambda x: x.id).collect()
	nid=[]
	a=[]
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	if usegraphframe:
		nid=[]
		a=[]
		# Get vertex list for serial iteration
		sc.setCheckpointDir("/tmp/graphframes-example-connected-components")
        	num_cc = g.connectedComponents().select('component').distinct().count()
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the outpt
		for i in vlist:
			edf = g.edges
			vdf = v.vertices
			edf = edf.filter(edf.src != i).filter(edf.dst != i)
			vdf = vdf.filter(vdf.id != i)
			g2 = GraphFrame(vdf,edf)
			temp_cc = g2.connectedComponents().select('component').distinct().count()
			if temp_cc > num_cc:
				nid.append(i)
				a.append(1)
			else:
				nid.append(i)
				a.append(0)
                df = sqlContext.createDataFrame(zip(nid,a),['id','articulation'])
		return df		
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
		G = nx.Graph()
		G.add_edges_from(elist)
		num_cc = nx.number_connected_components(G)
		for i in vlist:
			G.add_edges_from(elist)
			G.remove_node(i)
			temp_cc = nx.number_connected_components(G)
			if temp_cc > num_cc:
				nid.append(i)
				a.append(1)
			else:
				nid.append(i)
				a.append(0)
		df = sqlContext.createDataFrame(zip(nid,a),['id','articulation'])			
		return df		

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("---------------------------")
df.toPandas().to_csv("articulation_out.csv")
#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
