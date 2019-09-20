from igraph import * 
import pandas as pd
import numpy as np
import sys
from scipy.spatial import distance
'''
This sac1.py is required to run under python 3 enviorment.
'''
def cos_sim(v,u):
	return (1-distance.cosine(v,u))
	
def deltaQattr(g,simMatrix,m,i,j):
	s = 0.0
	old_i = m[i]
	m[i] = m[j]
	community = m[i]
	cl = Clustering(m)
	c_list = cl[community]
	for v in c_list:
		s = s + simMatrix[i][v]  
	
	m[i] = old_i
	return s/len(c_list)

def deltaQnewman(g,m,i,j):
	old_i = m[i]
	old = g.modularity(m)
	m[i] = m[j]
	new = g.modularity(m)
	m[i] = old_i
	return (new - old)

def deltaQ(g,alpha,simMatrix,membership,i,j):
	dQ = (alpha * deltaQnewman(g,membership,i,j)) + ( (1-alpha) * deltaQattr(g,simMatrix,membership,i,j))
	return dQ

def Phase1(g,alpha,simMatrix):
	# inital communities
	v = g.vcount()
	membership = [x for x in range(v)]
	check = v
	for l in range(15):
		for i in range(v):
			max_gain = 0.0
			gain = 0.0
			new_c = -1
			old_c = membership[i]
			for j in range(v):
				if i != j:
					gain = deltaQ(g,alpha,simMatrix,membership,i,j)
					if gain > max_gain:
						max_gain = gain 
						new_c = membership[j]

			if (max_gain > 0 and new_c >0):
				membership[i] = new_c
			else:
				membership[i] = old_c
		c_count = len(set(membership))
		#print (c_count)
		if check > c_count:
			check = c_count
		elif check == c_count:
			return membership		
	return membership

def vertex_mapping(cl1,cl2):
	final_cl = [[]*len(cl2) for x in range(len(cl2))]
	for i in range(len(cl2)):
		for j in cl2[i]:
			final_cl[i] = final_cl[i] + cl1[j]
	return final_cl
			

def save_to_txt(c, num):
	filename = "communities_" + str(num) +".txt"
	with open(filename, 'w') as f:
		for i in range(len(c)):
			for j in c[i]:
				f.write(str(j)+",")
			f.write("\n")
 
def main():
	# input alpha
	alpha = sys.argv[1]
	a = ['0','0.5','1']
	if alpha not in a or alpha is None:
		print ("please input alpha = 0 or  0.5 or  1")
		exit(1)

	# map alpha 
	if alpha == '0':
		alpha = 0
		com_num = 0
	elif alpha == '0.5':
		alpha = 0.5
		com_num = 5
	elif alpha == '1':
		alpha = 1
		com_num = 1
	# create graph from edgelsit
	g = Graph.Read_Edgelist("./data/fb_caltech_small_edgelist.txt",directed=False)
	
	# write attributes into vertices
	attri_df = pd.read_csv("./data/fb_caltech_small_attrlist.csv")
	acol_list = attri_df.columns.values
	for i in acol_list:
		g.vs[i] = attri_df[i]

	# initial similarity matrix
	node_count = g.vcount()
	simMatrix = [[]*node_count for x in range(node_count)]

	# compute similarity
	for i in range(node_count):
		for j in range(node_count):
			simMatrix[i].append( cos_sim(list(g.vs[i].attributes().values()),list(g.vs[j].attributes().values())))
	# phase1
	# add graph and similarity matrix into algorithm 
	m = Phase1(g,alpha,simMatrix)
	score1 = g.modularity(m)
	# clean the removed clusters and map new membership m2
	cl1 = list(Clustering(m))
	cl1 = list(filter(None,cl1))
	m2 = [None for x in range(node_count)]
	for i in range(len(cl1)):
		for j in cl1[i]:
			m2[j] = i
	# phase2
	# contact vertices and recompute similarity Matrix of contract vertices
	g.contract_vertices(m2,combine_attrs=mean)
	g.simplify(combine_edges=sum)
	node_count = g.vcount()
	simMatrix2 = [[]*node_count for x in range(node_count)]
	 
	for i in range(node_count):
		for j in range(node_count):
			sim_sum = 0.0
			for x in cl1[i]:
				for y in cl1[j]:
					sim_sum = sim_sum + simMatrix[x][y]
			simMatrix2[i].append(sim_sum)
	'''
	# normalization simMatrix2
	max_list = list(map(max,simMatrix2))
	for i in range(len(simMatrix2)):
		simMatrix2[i] = [ x/max_list[i] for x in simMatrix2[i]]
	'''
	final_m = Phase1(g,alpha,simMatrix2)
	score2 = g.modularity(final_m)
	cl2 = list(Clustering(final_m))
	cl2 = list(filter(None,cl2))
	if score1 > score2 and alpha == 1:
		final_cl = cl1
	else:
		final_cl = vertex_mapping(cl1,cl2)
	save_to_txt(sorted(final_cl),com_num)
 
if __name__ == "__main__":
	main()
	
