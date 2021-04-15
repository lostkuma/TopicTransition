# bash: main_run_clustering.py --include_removed_nodes True/False

import os 
import json 
import argparse
import datetime 
import numpy as np 
import networkx as nx 

from mcl_with_removal import FindOptimClustering 


def ComputeEdgeWeights(graph, num_tweets): 
	""" compute and set the normalized PMI weight for networkx graph 
		NOTE: the node freq and node pair co-occur freq are needed 
		pmi = log(p(x, y)/p(x)p(y)) = log(freq(x, y)*num_tweets/freq(x)freq(y))
		w = npmi = pmi / h(x, y) = pmi / (-log(p(x, y)))
	"""
	weights_mapping = dict() 
	
	for token1, token2 in graph.edges():
		freq1 = graph.nodes[token1]['freq']
		freq2 = graph.nodes[token2]['freq']
		freq_joint = graph.edges[token1, token2]['freq']
		
		w = np.log(freq_joint * num_tweets / (freq1 * freq2))
		norm = -1 * np.log(freq_joint / num_tweets)
		w = w / norm
		
		weights_mapping[(token1, token2)] = w
		
		nx.set_edge_attributes(graph, weights_mapping, 'weight')


def MakeTokenGraphsRaw():
	""" make graphs only with NN, NNS, NNP, NNPS, keep hashtags, NER
		nodes: the tokens 
		edges: the NPMI values
		make a graph for each day 
	"""
	graphs = list()
	daily_tweet_counts = list()

	for date in date_range: 
		
		graph = nx.Graph()
		count = 0 
		
		filepath = os.path.join(data_dir, 'processed_pu_' + date + '.json')
		pos_filepath = os.path.join(data_dir, 'pos_pu_' + date + '.json')
		ner_filepath = os.path.join(data_dir, 'ner_pu_' + date + '.json')
		
		with open(filepath, 'r', encoding='utf-8') as textfile:
			tweets_text = json.load(textfile)
			
		with open(pos_filepath, 'r', encoding='utf-8') as textfile:
			tweets_pos = json.load(textfile)
			
		with open(ner_filepath, 'r', encoding='utf-8') as textfile:
			tweets_ner = json.load(textfile)
	 
		for i in range(len(tweets_text)):
			# get unique tokens and compute weight between edges 
			tweet = tweets_text[i]
			pos = tweets_pos[i]
			ner = tweets_ner[i]
			
			# check if keywords in tweet, take only the tweets that have the keywords inside 
			if any(kw in tweet for kw in keywords):

				count += 1
				unique_tokens = set()
				
				for j in range(len(tweet)): 
					current_token = tweet[j]
					current_pos = pos[j]
					current_ner = ner[j]

					if current_pos in ['NN', 'NNS', 'NNP', 'NNPS'] or current_ner != 'O': 
						unique_tokens.add(current_token)

				unique_tokens = list(unique_tokens)

				for token in unique_tokens: # add unique tokens in tweet to graph
					if not graph.has_node(token):
						graph.add_node(token, freq=0)
					
					# update frequency of token, this frequency is number of tweet token appeared in 
					graph.nodes[token]['freq'] += 1
					
				if len(unique_tokens) > 1:
					for x in range(len(unique_tokens) - 1):
						for y in range(x+1, len(unique_tokens)):
							
							if not graph.has_edge(unique_tokens[x], unique_tokens[y]): 
								graph.add_edge(unique_tokens[x], unique_tokens[y], freq=0 )
							
							# update the frequency of the edges
							graph.edges[unique_tokens[x], unique_tokens[y]]['freq'] += 1
							
		graphs.append(graph)
		daily_tweet_counts.append(count)
		
	return graphs, daily_tweet_counts 


def AddRemovedNodesToClusters(graph, nodes_removed, clustering): 
	""" check if the removed tokens are connected with existing clusters and add them back 
		* modified_clustering - list of lists 
	"""
	modified_clustering = list() 
	
	for cluster in clustering: 
		new_cluster = set()
		for node in cluster: 
			neighbors = set(graph.neighbors(node)) 
			new_cluster.update(neighbors.intersection(nodes_removed))
		
		new_cluster.update(cluster)
		modified_clustering.append(list(new_cluster))

	return modified_clustering 


def RunClusteringMain(include_removed_nodes=False): 
	""" main function for running all the processes of clustering used in paper 
		try running for 20% of the total nodes for removal, and find best among 20% tried 
		dump metadata json output to result_dir
			* graph_nodes: the list of nodes of the graphs 
			* graph_edges: the list of edges (node pairs) and weight of the graphs 
			* best_subgraph： only a list of nodes make the best subgraph 
			* best_clustering: list of lists with the best clustering 
			* modularity_vluaes: a list of modularity values for all runs 
			* nodes_removed_best: the list of removed nodes when model achieves best 
	"""
	# construct and load graph 
	graphs, daily_tweet_counts = MakeTokenGraphsRaw()
	
	# add PMI edge weights to graph edges
	total_num_tweets = sum(daily_tweet_counts)
	for graph in graphs: 
		ComputeEdgeWeights(graph, total_num_tweets)
		
	if not os.path.isdir(result_dir): 
		os.mkdir(result_dir) 
		
	for i in range(len(date_range)): 
		date = date_range[i] 
		g = graphs[i] 
		output_filename = date + '_results_meta.json'
		
		# try 20% nodes removel 
		num_nodes_20 = int(g.number_of_nodes() * 0.2) 
		modularity_vals, highest_modularity, best_nodes_removed, best_subgraph, best_clustering = FindOptimClustering(g, iteration=num_nodes_20)
		
		if include_removed_nodes: 
			output_filename = date + '_results_meta_removed_included.json'
			best_clustering = AddRemovedNodesToClusters(g, best_nodes_removed, best_clustering)
			
		output_json = {'graph_nodes': dict(g.nodes.data()),
					   'graph_edges': list(g.edges.data()),
					   'best_subgraph': list(best_subgraph.nodes()), 
					   'best_clustering': best_clustering, 
					   'modularity_values': modularity_vals, 
					   'modularity_best': highest_modularity, 
					   'nodes_removed_best': best_nodes_removed, 
					  }

		# save to disk 
		with open(os.path.join(result_dir, output_filename), 'w', encoding='utf-8') as textfile: 
			json.dump(output_json, textfile, indent=2, ensure_ascii=True) 


def LoadClusteringResults(date_range, result_dir, include_removed_nodes=False):
	""" a load from disk function particular for loading results from RunClusteringMain() output """
	graphs = list() 
	best_clusterings = list() 
	graphs_metadata = list() 
	
	for date in date_range: 
		if include_removed_nodes: 
			filepath = os.path.join(result_dir, date + '_results_meta_removed_included.json') 
		else:
			filepath = os.path.join(result_dir, date + '_results_meta.json') 
	
		with open(filepath, 'r', encoding='utf-8') as textfile: 
			metadata = json.load(textfile)
			
		g = nx.Graph()
		g_metadata = {}
		
		g.add_edges_from(metadata['graph_edges'])
		g_metadata['modularity_vals'] = metadata['modularity_values']
		g_metadata['modularity_best'] = metadata['modularity_best']
		g_metadata['best_subgraph_nodes'] = metadata['best_subgraph']
		g_metadata['nodes_removed'] = metadata['nodes_removed_best']
		best_clustering = metadata['best_clustering']

		graphs.append(g) 
		graphs_metadata.append(g_metadata)
		best_clusterings.append(best_clustering)
		
	return graphs, best_clusterings, graphs_metadata 
	

if __name__=="__main__": 
	
	# constants
	data_dir = '../data/processed/' 
	original_dir = '../data/original/'
	result_dir = '../data/results/'
	figures_dir = '../data/figures/'
	
	# initialize date range for [0819, 0902]
	date_range = list()
	start = datetime.datetime.strptime("19-08-2020", "%d-%m-%Y")
	end = datetime.datetime.strptime("03-09-2020", "%d-%m-%Y")
	date_range_dt = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
	for dt in date_range_dt:
		str_date = str(dt.month).zfill(2) + str(dt.day).zfill(2) # pad casted string to 2 digit with leading 0s
		date_range.append(str_date)
		
	# top hashtags selected (freq >= 5 across whole dataset) that are related to the covid event 
	# refer to hashtag_freq.json
	# note that fetching keyword purdue related contents are not included 
	keywords = ['#protectpurdue', '#covid19', '#coronavirus', '#inthistogether', '#covid', '#maskup', '#flu', '#pandemic', '#healthforall', '#masks']
	
	# argument from commandline 
	parser = argparse.ArgumentParser(description='clustering parameters')
	parser.add_argument('--include_removed_nodes', type=bool, default=False, help='boolean to choose whether to include removed nodes in cluster results')
	args = parser.parse_args() 
	
	# run clusters 
	RunClusteringMain(include_removed_nodes=args.include_removed_nodes)

	# LoadClusteringResults(date_range, result_dir) 
	
	