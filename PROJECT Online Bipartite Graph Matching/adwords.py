import sys
import pandas as pd
import numpy as np
import random
import time

def greedy(bid_dict,query,budget_dict):
	revenue = 0.0
	for q in query:
		match = bid_dict[q]
		aid = list(match.keys())
		a_list=[]
		b_list=[]
		#check budget
		for i in aid:
			bid = match[i]
			if budget_dict[i] >= bid :
				a_list.append(i)
				b_list.append(bid)
		if not a_list:
			continue
		else:
			# find max value and store smallest id
			max_dict = dict(zip(a_list,b_list))
			keys = list(max_dict.keys())
			max_bid = max(max_dict.values())
			c_list=[]
			for k in keys:
				if max_dict[k] == max_bid:
					c_list.append(k)
			ad_id = min(c_list) 
			#updates revenue and advertiser's budget
			budget_dict[ad_id] = budget_dict[ad_id] - max_bid
			revenue += max_bid
	return revenue

def msvv(bid_dict,query,budget_dict):
	revenue = 0.0
	rembudget_dict = budget_dict.copy()
	for q in query:
		match = bid_dict[q]
		aid = list(match.keys())
		a_list=[]
		b_list=[]
		#check budget
		for i in aid:
			bid = match[i]
			if rembudget_dict[i] >= bid :
				a_list.append(i)
				b_list.append(bid)
		if not a_list:
			continue
		else:
			max_value = 0.0
			for k in a_list:
				y = msvv_y(budget_dict[k],rembudget_dict[k],match[k])
				if y > max_value:
					ad_id = k
					max_value = y
				elif y == max_value:
					if ad_id > k:
						ad_id = k
						max_value = y
			rembudget_dict[ad_id] = rembudget_dict[ad_id] - match[ad_id]
			revenue += match[ad_id]
	return revenue

def msvv_y(budget,rembudget,bid):
	Xu = (budget-rembudget) / budget
	y = 1-(np.exp(Xu-1))
	return y*bid

def balance(bid_dict,query,budget_dict):
	revenue = 0.0
	for q in query:
		match = bid_dict[q]
		aid = list(match.keys())
		a_list=[]
		bd_list=[]
		#check budget
		for i in aid:
			bid = match[i]
			if budget_dict[i] >= bid :
				a_list.append(i)
				bd_list.append(budget_dict[i])
		if not a_list:
			continue
		else:
			max_dict = dict(zip(a_list,bd_list))
			keys = list(max_dict.keys())
			max_budget = max(max_dict.values())
			c_list=[]
			for k in keys:
				if max_dict[k] == max_budget:
					c_list.append(k)
			ad_id = min(c_list)
			max_bid = match[ad_id]
			budget_dict[ad_id] = budget_dict[ad_id] - max_bid
			revenue += max_bid

	return revenue


def main():
	if len(sys.argv) != 2:
		print("input error!, please intput alogrithm arg('greedy','msvv','balance')")
		exit(1)
	else:
		random.seed(0)
		arg = sys.argv[1]
		#retrieve the data
		bidder_data = pd.read_csv('bidder_dataset.csv')
		with open('queries.txt') as f:
			query = f.read().splitlines()
		#get advertiser id and its budget
		budget_data = bidder_data.dropna()
		a_id = budget_data['Advertiser']
		budget = budget_data['Budget']
		budget_dict = dict(zip(a_id,budget))
		opt = sum(budget_dict.values())		
		#create dict for keyword as main key, ad_id as secondary key, bid as values 
		bid_dict={}
		for i in range(len(bidder_data)):
			keys = bidder_data.iloc[i]['Keyword']
			aid = bidder_data.iloc[i]['Advertiser']
			bid_value = bidder_data.iloc[i]['Bid Value'] 
			if not (keys in bid_dict):
				bid_dict[keys]={}
			if not (aid in bid_dict[keys]):
				bid_dict[keys][aid]=bid_value
				
		#algorithm chosen
		if arg == 'greedy':
			start_time = time.time()
			temp_r = 0.0
			revenue = greedy(bid_dict,query,budget_dict)
			for i in range(100):
				#reset budget
				budget_dict = dict(zip(a_id,budget))
				random.shuffle(query)
				r = greedy(bid_dict,query,budget_dict)
				temp_r += r
			alg = temp_r/100
		elif arg == 'msvv':
			start_time = time.time()
			temp_r = 0.0
			revenue = msvv(bid_dict,query,budget_dict)
			for i in range(100):
				#reset budget
				budget_dict = dict(zip(a_id,budget))
				random.shuffle(query)
				r = msvv(bid_dict,query,budget_dict)
				temp_r += r
			alg = temp_r/100
		elif arg == 'balance':
			start_time = time.time()
			temp_r = 0.0
			revenue = balance(bid_dict,query,budget_dict)
			for i in range(100):
				#reset budget
				budget_dict = dict(zip(a_id,budget))
				random.shuffle(query)
				r = balance(bid_dict,query,budget_dict)
				temp_r += r
			alg = temp_r/100
		
		ratio = alg / opt
		print("revenue:"+str(round(revenue,2)))
		print("competitive ratio:"+str(round(ratio,2)))
if __name__ == "__main__":
	main()
