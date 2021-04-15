# bash: python main_trace_transition.py --mode crisp/fuzzy --data computer/human

import os 
import json
import argparse
import datetime
import numpy as np
import networkx as nx

from main_run_clustering import LoadClusteringResults
from cluster_transition_graph_config import ClusterNode, GraphEdge, Graph
from pairwise_cluster_transition import FindMatchingClustersMain, MatchReappearingClusters
from fuzzy_transition import MakeTransitionGraphFuzzy, ComputeFuzzySets


# cluster id functions 
def GetClusterID(timepoint, cluster_idx):
	return str(timepoint) + '_' + str(cluster_idx).zfill(2)

def GetTimepointAndClusterIdx(node_id): 
	node_id = node_id.split('_')
	return node_id[0], int(node_id[1])
	
	
def FindPairwiseTransitionsCrisp(date_range, clustering_by_timepoint, threshold_passed=2/3, threshold_criteria=2/3):
	""" for each day's clustering, return the pairwise transition mapping 
		* pairwise_date_to_transition_mapping - a dict: {timepoint_tuple: transition_dict}
			-- {(timepoint1, timepoint2): {'unchanged': {cl_idx_t1: cl_idx_t2, ...}, 
											'absorbed': {cl_idx_t1: cl_idx_t2, ...},
											'split': {cl_idx_t1 : [cl_idx_t2, cl_idx_t2, ...]}, 
											'dissolved': {cl_idx_t1: cl_idx_t2, ...}, 
											'merged': {(cl_idx_t1, cl_idx_t1, ...): cl_idx_t2, ...}
											}
				, .....
				}
	"""
	pairwise_date_to_transition_mapping = dict() 

	for i in range(len(date_range) - 1):
		clustering_a = clustering_by_timepoint[i]
		clustering_b = clustering_by_timepoint[i + 1]
		date1 = date_range[i]
		date2 = date_range[i + 1]
		date_key = (date1, date2)

		matching = FindMatchingClustersMain(clustering_a, clustering_b, threshold_passed, threshold_criteria)
		pairwise_date_to_transition_mapping[date_key] = matching 
		
	return pairwise_date_to_transition_mapping


def MakeTransitionGraph(date_range, clustering_by_timepoint, pairwise_date_to_transition_mapping):
	""" make transition graph for 5 basic types of pairwise transitions: 
			- unchanged, absorbed, split, dissplved, merged
			
		* transition_graph: Graph obj from cluster_transition_graph_config
	"""
	transition_graph = Graph() 

	for i in range(len(date_range)): 
		clustering = clustering_by_timepoint[i]
		timepoint = date_range[i]

		for cl_idx in range(len(clustering)):
			cluster_elements = clustering[cl_idx]
			transition_graph.AddNode(cluster_elements, timepoint, cl_idx)

	for (timepoint1, timepoint2), transition_meta in pairwise_date_to_transition_mapping.items(): 
		unchanged_matching = transition_meta['unchanged'] 
		absorbed_matching = transition_meta['absorbed']
		split_matching = transition_meta['split']
		dissolved_matching = transition_meta['dissolved']
		merged_matching = transition_meta['merged']
		
		if len(unchanged_matching) > 0: 
			for cl_a_idx, cl_b_idx in unchanged_matching.items(): 
				cluster_1 = transition_graph.GetNodeByTimepointAndIndex(timepoint1, cl_a_idx)
				cluster_2 = transition_graph.GetNodeByTimepointAndIndex(timepoint2, cl_b_idx)
				transition_graph.AddDirectedEdge(cluster_1, cluster_2, type='unchanged')

		if len(absorbed_matching) > 0: 
			for cl_a_idx, cl_b_idx in absorbed_matching.items(): 
				cluster_1 = transition_graph.GetNodeByTimepointAndIndex(timepoint1, cl_a_idx)
				cluster_2 = transition_graph.GetNodeByTimepointAndIndex(timepoint2, cl_b_idx)
				transition_graph.AddDirectedEdge(cluster_1, cluster_2, type='absorbed')

		if len(split_matching) > 0: 
			for cl_a_idx, cl_b_idxs in split_matching.items(): 
				for cl_b_idx in cl_b_idxs:
					cluster_1 = transition_graph.GetNodeByTimepointAndIndex(timepoint1, cl_a_idx)
					cluster_2 = transition_graph.GetNodeByTimepointAndIndex(timepoint2, cl_b_idx)
					transition_graph.AddDirectedEdge(cluster_1, cluster_2, type='split')

		if len(dissolved_matching) > 0: 
			for cl_a_idx, cl_b_idx in dissolved_matching.items(): 
				cluster_1 = transition_graph.GetNodeByTimepointAndIndex(timepoint1, cl_a_idx)
				cluster_2 = transition_graph.GetNodeByTimepointAndIndex(timepoint2, cl_b_idx)
				transition_graph.AddDirectedEdge(cluster_1, cluster_2, type='dissolved')			

		if len(merged_matching) > 0: 
			for cl_a_idxs, cl_b_idx in merged_matching.items(): 
				for cl_a_idx in cl_a_idxs:
					cluster_1 = transition_graph.GetNodeByTimepointAndIndex(timepoint1, cl_a_idx)
					cluster_2 = transition_graph.GetNodeByTimepointAndIndex(timepoint2, cl_b_idx)
					transition_graph.AddDirectedEdge(cluster_1, cluster_2, type='merged')
				
	return transition_graph


def AddReappearClusters(graph, timepoint_to_idx_mapping, threshold=1/2): 
	""" add reappearing clusters to the base pairwise transition graph 
		reappear clusters are matched through the last cluster in pairwise sequence 
	"""
	for current_timepoint, current_timepoint_idx in timepoint_to_idx_mapping.items():
		for current_node in graph.GetNodesAtTimepoint(current_timepoint):
			# if a node has outgoing neighbors, continue 
			if current_node.HasOutgoingNeighbors(): 
				continue 
			# if a node does not have outgoing neighbors, try to find a match 
			else: 
				current_cluster = current_node.GetElements() 
				if current_timepoint_idx < len(timepoint_to_idx_mapping) - 3:
					timepoints_after_current = list(timepoint_to_idx_mapping.keys())[current_timepoint_idx + 2:]
				
					for timepoint_b in timepoints_after_current:
						clustering_b = list() # clustering b of the timepoint b

						for clustering_b_node in graph.GetSortedNodesAtTimepoint(timepoint_b): 
							clustering_b.append(clustering_b_node.GetElements())

						matching_idx = MatchReappearingClusters(current_cluster, clustering_b, threshold)

						if matching_idx: 
							matched_node = graph.GetNodeByTimepointAndIndex(timepoint_b, matching_idx)
							graph.AddReappearEdge(current_node, matched_node)
							break


def MakeTransitionSubgraph(graph, clustering_by_timepoint, date_range, include_reappear=False, include_single_node_subgraph=False): 
	""" Get all transition subgraphs from the transition grpah 
		* all_transition_subgraphs - [Graph obj, ...]
	"""
	all_transition_subgraphs = list()

	for i, timepoint in enumerate(date_range): 
		for cl_idx, cluster in enumerate(clustering_by_timepoint[i]): 
			cluster_id = GetClusterID(timepoint, cl_idx) 

			transition_subgraph = graph.GetTransitionSubgraphByNodeID(cluster_id, include_reappear)
			
			if not include_single_node_subgraph:
				if transition_subgraph.GetNumOfNodes() < 2: 
					continue 
			
			if transition_subgraph not in all_transition_subgraphs: 
				all_transition_subgraphs.append(transition_subgraph)

	return all_transition_subgraphs 


def GetCrispTransitionSubgraphs(clustering_by_timepoint, date_range, include_reappear=True, reappear_threshold=1/2, include_single_node_subgraph=False): 
	""" Similar to GetCrispTransitionTuples() but output different forms
		Get all crisp transition subgraphs from the transition grpah 
		* all_transition_subgraphs - [Graph obj, ...]
	"""
	# find pairwise transitions 
	pairwise_date_to_transition_mapping = FindPairwiseTransitionsCrisp(date_range, clustering_by_timepoint)

	# make cluster transition graph 
	transition_graph = MakeTransitionGraph(date_range, clustering_by_timepoint, pairwise_date_to_transition_mapping)

	if include_reappear: 
		# add reappear clusters to base transition graph 
		AddReappearClusters(transition_graph, timepoint_to_idx_mapping, threshold=reappear_threshold) 
		
	# get all transition subgraphs with transition sequence length > 1
	all_transition_subgraphs = MakeTransitionSubgraph(transition_graph, clustering_by_timepoint, date_range, include_reappear=include_reappear, include_single_node_subgraph=include_single_node_subgraph)
	
	return all_transition_subgraphs
	
	
def GetCrispTransitionTuples(clustering_by_timepoint, date_range, include_reappear=True, reappear_threshold=1/2): 
	""" Similar to GetCrispTransitionSubgraphs but output different forms, cannot include single node
		Find all crisp transitions, output a file containing tuples 
		* list_of_node_tuples - to be consistent with fuzzy transition, tuple is in the following format: 
			(cl_idx_1, cl_idx_2, transition_type, 'strong', 1) 
	"""
	list_of_node_tuples = list() 
	
	# find pairwise transitions 
	pairwise_date_to_transition_mapping = FindPairwiseTransitionsCrisp(date_range, clustering_by_timepoint)

	# make cluster transition graph 
	transition_graph = MakeTransitionGraph(date_range, clustering_by_timepoint, pairwise_date_to_transition_mapping)

	if include_reappear: 
		# add reappear clusters to base transition graph 
		AddReappearClusters(transition_graph, timepoint_to_idx_mapping, threshold=reappear_threshold) 
		
	for timepoint in date_range: 
		for node in transition_graph.GetNodesAtTimepoint(timepoint): 
			current_node_id = node.GetID() 
			
			for neighbor_id, edge in node.GetOutgoingNeighborsAndEdges(): 
				transition_type = edge.GetEdgeType()
				list_of_node_tuples.append((current_node_id, neighbor_id, transition_type, 'strong', 1))
	
	return list_of_node_tuples
	
	
def GetFuzzyTransitionTuples(clustering_by_timepoint, date_range, fuzzy_limiter=[0.3, 0.4, 0.6, 0.7]): 
	"""	Find all fuzzy transitions, output a file containing tuples 
		reappear transitions are not currently supported 
		* list_of_node_tuples - tuple is in the following format: 
			(cl_idx_1, cl_idx_2, transition_type, strength, membership_miu) 
	"""
	assert len(fuzzy_limiter) == 4 and any(type(i) in [float, int] for i in fuzzy_limiter), 'fuzzy_limiter needs to be length 4 iterable with float!'
	
	list_of_node_tuples = list() 
	
	# make fuzzy transition graph 
	transition_graph_fuzzy = MakeTransitionGraphFuzzy(date_range, clustering_by_timepoint)
	
	for timepoint in date_range: 
		for node in transition_graph_fuzzy.GetNodesAtTimepoint(timepoint): 
			current_node_id = node.GetID() 
			for neighbor_id, edge in node.GetOutgoingNeighborsAndEdges(): 
				fuzzy_type_to_x_mapping = edge.GetFuzzytypes()
				for fuzzy_type, x in fuzzy_type_to_x_mapping.items(): 
					fuzzy_sets = ComputeFuzzySets(x, fuzzy_limiter[0], fuzzy_limiter[1], fuzzy_limiter[2], fuzzy_limiter[3])
					for strength, miu in fuzzy_sets: 
						list_of_node_tuples.append((current_node_id, neighbor_id, fuzzy_type, strength, miu))
	
	return list_of_node_tuples 

	
if __name__=='__main__': 
	
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

	# argument from commandline 
	parser = argparse.ArgumentParser(description='cluster transition parameters')
	parser.add_argument('--mode', type=str, default='crisp', help='choose whether transition mode is crisp or fuzzy')
	parser.add_argument('--data', type=str, default='computer', help='choose whether computer generated data or human labeled data')
	args = parser.parse_args() 
	assert args.mode in ['crisp', 'fuzzy'], 'mode needs to be either "crisp" or "fuzzy"!'
	assert args.data in ['computer', 'human'], 'data needs to be either "computer" or "human"!'

	# timepoint to index mapping for date range 
	timepoint_to_idx_mapping = dict(zip(date_range, range(len(date_range))))
	
	# load graph and clustering results by day, choose to load either computer or human generated clusters 
	if args.data == 'computer': 
		graphs, clustering_by_timepoint, graphs_metadata = LoadClusteringResults(date_range, result_dir, include_removed_nodes=True)
	
	elif args.data == 'human': 
		human_label_path = os.path.join(result_dir, 'tweet_label_sets.txt')
		with open(human_label_path, 'r', encoding='utf-8') as textfile: 
			date_to_labels_mapping = json.load(textfile)
		clustering_by_timepoint = list(date_to_labels_mapping.values())
	
	# choose mode 
	if args.mode == 'crisp':
		# get all transition subgraphs with transition sequence length > 1
		output_filename = 'crisp_graph_tuples.json'
		list_of_node_tuples = GetCrispTransitionTuples(clustering_by_timepoint, date_range, include_reappear=True, reappear_threshold=2/3)
			
	elif args.mode == 'fuzzy': 
		output_filename = 'fuzzy_graph_tuples.json'
		list_of_node_tuples = GetFuzzyTransitionTuples(clustering_by_timepoint, date_range, fuzzy_limiter=[0.3, 0.4, 0.6, 0.7])
		
	# output the transition tuples to file 
	with open(os.path.join(result_dir, output_filename), 'w', encoding='utf-8') as textfile: 
		json.dump(list_of_node_tuples, textfile, indent=2) 

