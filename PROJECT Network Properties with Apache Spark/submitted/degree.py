import sys
import pandas
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from graphframes import *
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

'''return the simple closure of the graph as a graphframe.'''
def simple(g):
    # Extract edges and make a data frame of "flipped" edges    
    df = g.edges
    src = df.select('src').flatMap(lambda x: x).collect()
    dst = df.select('dst').flatMap(lambda x: x).collect()
    fe_df = sqlContext.createDataFrame(zip(dst,src),['src','dst'])
    # Combine old and new edges. Distinctify to eliminate multi-edges
    # Filter to eliminate self-loops.
    # A multigraph with loops will be closured to a simple graph
    # If we try to undirect an undirected graph, no harm done
    edf = df.unionAll(fe_df).distinct().dropDuplicates(['src','dst'])

    g2 = GraphFrame(g.vertices,edf)
    return g2
    ''' 
    Return a data frame of the degree distribution of each edge in
    the provided graphframe. 
    '''
def degreedist(g):
    # Generate a DF with degree,count
    degreeDF=g.degrees
    df = degreeDF.groupBy('degree').count().orderBy('degree')
    degree = df.select('degree').flatMap(lambda x: x).collect()
    j=0
    for i in degree:
       degree[j] = i/2
       j=j+1 
    count = df.select('count').flatMap(lambda x: x).collect()
    df2 = sqlContext.createDataFrame(zip(degree,count),['degree','count']).sort('degree')
    return df2
    '''
    Read in an edgelist file with lines of the format id1<delim>id2
    and return a corresponding graphframe. If "large" we assume
    a header row and that delim = " ", otherwise no header and
    delim = ","
    '''
def readFile(filename, large, sqlContext=sqlContext):
    lines = sc.textFile(filename)
    if large:
        delim=" "
        # Strip off header row.
        lines = lines.mapPartitionsWithIndex(lambda ind,it: iter(list(it)[1:]) if ind==0 else it)
    else:
        delim=","

    # Extract pairs from input file and convert to data frame matching
    # schema for graphframe edges.
    #eschema = StructType([StructField("src", IntegerType()),StructField("dst", IntegerType())])
    #e = lines.map(lambda x: x.split(delim)).map(lambda x: (int(x[0]),int(x[1]))).toDF(eschema)
    e = lines.map(lambda x: x.split(delim)).map(lambda x: (int(x[0]),int(x[1])))
    edf = sqlContext.createDataFrame(e,['src','dst'])
 
    # Extract all endpoints from input file (hence flatmap) and create
    # data frame containing all those node names in schema matching
    # graphframe vertices
    #vschema = StructType([StructField("id", IntegerType())])
    #v = lines.flatMap(lambda x: x.split(delim)).map(lambda x: (int(x),)).toDF(vschema)
    v=lines.flatMap(lambda x: x.split(delim)).map(lambda x: (int(x),))
    vdf=sqlContext.createDataFrame(v,['id'])
    # Create graphframe g from the vertices and edges.
    g = GraphFrame(vdf,edf)

    return g


# main stuff

# If you got a file, yo, I'll parse it.
if len(sys.argv) > 1:
    filename = sys.argv[1]
    if len(sys.argv) > 2 and sys.argv[2]=='large':
        large=True
    else:
        large=False

    print("Processing input file " + filename)
    g = readFile(filename, large)

    print("Original graph has " + str(g.edges.count()) + " directed edges and " + str(g.vertices.count()) + " vertices.")

    g2 = simple(g)
    print("Simple graph has " + str(g2.edges.count()/2) + " undirected edges.")

    distrib = degreedist(g2)
    distrib.show()
    nodecount = g2.vertices.count()
    print("Graph has " + str(nodecount) + " vertices.")

    out = filename.split("/")[-1]
    print("Writing distribution to file " + out + ".csv")
    distrib.toPandas().to_csv(out + ".csv")
    #plot
    fig = plt.figure()
    degree = distrib.select('degree').flatMap(lambda x: x).collect()
    clist =  distrib.select('count').flatMap(lambda x: x).collect()
    sum_count = sum(clist)
    freqlist = distrib.select('count').flatMap(lambda x: x).collect()
    freqlist = [float(i) for i in freqlist]
    fraction = [ x/sum_count for x in freqlist]
    plt.plot(degree,fraction)
    fig.savefig(out+".png")
# Otherwise, generate some random graphs.
else:
    print("Generating random graphs.")
    vschema = StructType([StructField("id", IntegerType())])
    eschema = StructType([StructField("src", IntegerType()),StructField("dst", IntegerType())])

    gnp1 = nx.gnp_random_graph(100, 0.05, seed=1234)
    gnp2 = nx.gnp_random_graph(2000, 0.01, seed=5130303)
    gnm1 = nx.gnm_random_graph(100,1000, seed=27695)
    gnm2 = nx.gnm_random_graph(1000,100000, seed=9999)

    todo = {"gnp1": gnp1, "gnp2": gnp2, "gnm1": gnm1, "gnm2": gnm2}
    for gx in todo:
        print("Processing graph " + gx)
        v = sqlContext.createDataFrame(sc.parallelize(todo[gx].nodes()), vschema)
        e = sqlContext.createDataFrame(sc.parallelize(todo[gx].edges()), eschema)
        g = simple(GraphFrame(v,e))
        distrib = degreedist(g)
        print("Writing distribution to file " + gx + ".csv")
        distrib.toPandas().to_csv(gx + ".csv")
#plot
        fig = plt.figure()
        degree = distrib.select('degree').flatMap(lambda x: x).collect()
        clist =  distrib.select('count').flatMap(lambda x: x).collect()
        sum_count = sum(clist)
        freqlist = distrib.select('count').flatMap(lambda x: x).collect()
        freqlist = [float(i) for i in freqlist]
        fraction = [ x/sum_count for x in freqlist]
        plt.plot(degree,fraction)
        fig.savefig(gx+".png")
