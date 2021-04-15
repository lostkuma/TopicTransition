# contains clustering with MCL 
# FindOptimClustering 

import networkx as nx 
from networkx.algorithms.community import modularity as Modularity

# markov clustering github implementation 
# https://github.com/GuyAllard/markov_clustering
import markov_clustering as mc

def AdjacencyMatrix(graph): 
	""" return adjacency matrix and the idx to token mapping """
	num_of_nodes = graph.number_of_nodes() 
	all_tokens = sorted(graph.nodes())
	idx_to_token_mapping = dict(zip(range(num_of_nodes), all_tokens))
	adj_mat = nx.adjacency_matrix(graph, nodelist=all_tokens, weight='weight') 
	
	return adj_mat, idx_to_token_mapping

		
def GetSortedClusteringCoeff(graph): 
	""" sort tokens by lower to higher clustering coefficient"""
	for token, coeff in sorted(nx.clustering(graph).items(), key=lambda x: x[1]):
		yield token, coeff


def GetCurrentSubgraph(graph, current_high_degree_nodes): 
	""" return current subgraph given a set of nodes to be excluded """
	return graph.subgraph(graph.nodes() - current_high_degree_nodes)
	
	
def MakePermutationDict(input_dict, keys, current_key_idx, current_permutation, output_permutations): 
	""" make permuted dictionaries given an input dict 
		input format: {token1:[0, 1], token2:[0, 3, 4], token3:[0, 3], token4:[1, 3]}
		output format: iterator of dict i.e. {token1:0, token2:0, token3:0, token4:1}
	"""
	if current_key_idx == len(keys): 
		output_permutations.append(current_permutation)
		return 
	
	current_key = keys[current_key_idx]
	
	for opt in input_dict[current_key]: 
		updated_permutation = current_permutation.copy() 
		updated_permutation[current_key] = opt 
		
		MakePermutationDict(input_dict, keys, current_key_idx + 1, updated_permutation, output_permutations)
		

def EnforceOneToOneMapping(graph, token_clusters):
	""" for isomophic clusters in MCL, try out all combinations 
		find the best combination to enforce element with 1to1 cluster mapping 
		best combination is determined by the highest modularity value 
	"""
	token_to_cluster = dict(zip(graph.nodes(), [[] for i in range(graph.number_of_nodes())]))
	num_clusters = len(token_clusters)
	max_modularity = -1 # modularity is a val between [-1, 1]
	best_clustering = None

	for i in range(len(token_clusters)):
		for token in token_clusters[i]: 
			token_to_cluster[token].append(i)

	# separate repeating and none repeating tokens 
	repeating_token_to_cluster = dict()
	none_repeating_token_to_cluster = dict() 
	
	for token, cl_idx in token_to_cluster.items(): 
		if len(cl_idx) <= 1:
			none_repeating_token_to_cluster[token] = cl_idx[0]
		else:
			repeating_token_to_cluster[token] = cl_idx 
	
	# make permutations for the repeating tokens to get all combinations of the isomorphic clusters 
	all_combinations = list()
	MakePermutationDict(repeating_token_to_cluster, list(repeating_token_to_cluster.keys()), 0, {}, all_combinations)
	
	# update on the combination so none repeating dict remains unchanged 
	for comb_dict in all_combinations: 
		comb_dict.update(none_repeating_token_to_cluster)
		
		# obtain the current version of the clustering 
		current_clustering = [[] for i in range(num_clusters)]
		for token, cl_idx in comb_dict.items(): 
			current_clustering[cl_idx].append(token)
			
		# compute the current modularity, update if the current modularity is the best 
		current_modularity = Modularity(graph, current_clustering)
		if current_modularity > max_modularity: 
			max_modularity = current_modularity
			best_clustering = current_clustering
			
	return best_clustering, max_modularity


def RunMCL(graph): 
	""" run markove clustering once """
	# get adjacency matrix and mapping, then run mcl
	adj_mat, idx_to_token_mapping = AdjacencyMatrix(graph)
	adj_mat = adj_mat.toarray() # from sparse to np array
	mcl_clustering = mc.run_mcl(adj_mat, inflation=2)
	clusters = mc.get_clusters(mcl_clustering)

	# get token representation of the clusters 
	token_clusters = list() 
	for cl in clusters:
		cl_tokens = list()
		for idx in cl:
			cl_tokens.append(idx_to_token_mapping[idx])
		token_clusters.append(cl_tokens)
		
	# check if repeating node exists due to isomorphic graph structures 
	num_nodes_in_clusters = sum(len(c) for c in token_clusters)
	if graph.number_of_nodes() != num_nodes_in_clusters:
		token_clusters, modularity = EnforceOneToOneMapping(graph, token_clusters)				
	else: 
		modularity = Modularity(graph, token_clusters)
		
	return token_clusters, modularity
	

def FindOptimClustering(graph, iteration=100): 
	""" run clustering by gradually removing nodes on one graph to find the best number of nodes to remove """
	
	current_high_degree_nodes = list() 
	modularity_vals = list()
	highest_modularity = -1 
	num_nodes_removed = 0
	best_subgraph = None
	best_clutering = None 
	best_nodes_removed = None
	
	for token, coeff in GetSortedClusteringCoeff(graph): 
		current_subgraph = GetCurrentSubgraph(graph, current_high_degree_nodes)	   
		clusters, modularity = RunMCL(current_subgraph) 
		modularity_vals.append(modularity)
		
		if modularity > highest_modularity: 
			best_subgraph = current_subgraph
			best_clustering = clusters
			highest_modularity = modularity 
			best_nodes_removed = current_high_degree_nodes.copy() # deep copy

		current_high_degree_nodes.append(token)
		num_nodes_removed += 1 
		
		if num_nodes_removed > iteration: 
			break 

	return modularity_vals, highest_modularity, best_nodes_removed, best_subgraph, best_clustering
