# fuzzy cluster transitions 
# MakeTransitionGraphFuzzy, ComputeFuzzySets

from cluster_transition_graph_config import ClusterNode, GraphEdge, Graph


def MakeTransitionGraphRaw(list_of_timepoint, clustering_by_timepoint): 
	transition_graph = Graph() 

	for i in range(len(list_of_timepoint)): 
		clustering = clustering_by_timepoint[i]
		timepoint = list_of_timepoint[i]

		for cl_idx in range(len(clustering)):
			cluster_elements = clustering[cl_idx]
			transition_graph.AddNode(cluster_elements, timepoint, cl_idx)
	
	for i in range(len(list_of_timepoint) - 1): 
		current_timepoint = list_of_timepoint[i]
		next_timepoint = list_of_timepoint[i + 1]
		for node_a in transition_graph.GetNodesAtTimepoint(current_timepoint): 
			current_elements = node_a.GetElements()
			current_node_id = node_a.GetID() 
			
			for node_b in transition_graph.GetNodesAtTimepoint(next_timepoint): 
				next_elements = node_b.GetElements() 
				next_node_id = node_b.GetID() 
				intersection = current_elements.intersection(next_elements)

				if len(intersection) > 0:
					transition_graph.AddIntersectingEdge(current_node_id, next_node_id, intersection)
					transition_graph.AddDirectedEdge(node_a, node_b, type='fuzzy')
			
	return transition_graph 


def XDisappear(node, transition_graph, timepoint, list_of_timepoint):
	""" compute the disappear core x between current timepoint and next timepoint 
		* return x value, if not None 
	"""
	# determine next timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx < len(list_of_timepoint) - 1:
		next_timepoint = list_of_timepoint[current_timepoint_idx + 1]
	else: return 0
	
	# find number of element passed to next timepoint 
	element_passed_to_next = set()
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements(): 
		if neighbor_id.startswith(next_timepoint): 
			element_passed_to_next.update(intersection)
			
	num_element_passed = len(element_passed_to_next)
	num_elements_in_cluster = len(node.GetElements())
	x = 1 - num_element_passed / num_elements_in_cluster
	return x 

def XUnchange(node, transition_graph, timepoint, list_of_timepoint): 
	""" compute the unchanged core x between current timepoint and next timepoint 
		* return x value, if not None 
	"""
	# determine next timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx < len(list_of_timepoint) - 1:
		next_timepoint = list_of_timepoint[current_timepoint_idx + 1]
	else: return
	
	# check the number of intersecting neighbors 
	num_intersecting_neighbors = node.GetNumOfIntersectingNeighbors()
	if num_intersecting_neighbors <= 0: 
		return 
	
	# find the mapping between each unchanged cluster and the x	 
	node_id_to_x_mapping = dict()
	cluster_elements = node.GetElements()
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements():
		if neighbor_id.startswith(next_timepoint): 
			neighbor_elements = transition_graph.GetNodeByID(neighbor_id).GetElements()
			union_clusters = set(cluster_elements).union(set(neighbor_elements))
			x = len(intersection) / len(union_clusters)
			node_id_to_x_mapping[neighbor_id] = x
			
	return node_id_to_x_mapping

def XAbsorb(node, transition_graph, timepoint, list_of_timepoint): 
	""" compute the absorb core x between current timepoint and next timepoint 
		* return x value, if not None 
	"""
	# determine next timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx < len(list_of_timepoint) - 1:
		next_timepoint = list_of_timepoint[current_timepoint_idx + 1]
	else: return
	
	# check the number of intersecting neighbors 
	num_intersecting_neighbors = node.GetNumOfIntersectingNeighbors()
	if num_intersecting_neighbors <= 0: 
		return 

	# find the mapping between each absorbed cluster and the x	
	node_id_to_x_mapping = dict()
	cluster_elements = node.GetElements()
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements():
		if neighbor_id.startswith(next_timepoint): 
			neighbor_elements = transition_graph.GetNodeByID(neighbor_id).GetElements()
			union_clusters = set(cluster_elements).union(set(neighbor_elements))
			x = len(intersection) / len(cluster_elements) - len(intersection) / len(union_clusters)
			if x <= 0: 
				continue 
			node_id_to_x_mapping[neighbor_id] = x 

	return node_id_to_x_mapping	 

def XDissolve(node, transition_graph, timepoint, list_of_timepoint): 
	""" compute the dissolve core x between current timepoint and next timepoint  
		* return x value, if not None 
	"""
	# determine next timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx < len(list_of_timepoint) - 1:
		next_timepoint = list_of_timepoint[current_timepoint_idx + 1]
	else: return
	
	# check the number of intersecting neighbors 
	num_intersecting_neighbors = node.GetNumOfIntersectingNeighbors()
	if num_intersecting_neighbors <= 0: 
		return 
	
	# find the mapping between each dissolved cluster and the x 
	node_id_to_x_mapping = dict()
	cluster_elements = node.GetElements()
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements():
		if neighbor_id.startswith(next_timepoint): 
			neighbor_elements = transition_graph.GetNodeByID(neighbor_id).GetElements()
			union_clusters = set(cluster_elements).union(set(neighbor_elements)) 
			x = len(intersection) / len(neighbor_elements) - len(intersection) / len(union_clusters)
			if x <= 0: 
				continue 
			node_id_to_x_mapping[neighbor_id] = x 
			
	return node_id_to_x_mapping	 

def XSplit(node, transition_graph, timepoint, list_of_timepoint): 
	""" compute the split core x between current timepoint and next timepoint  
		* return x value, if not None 
	"""
	# determine next timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx < len(list_of_timepoint) - 1:
		next_timepoint = list_of_timepoint[current_timepoint_idx + 1]
	else: return
	
	# check the number of intersecting neighbors 
	num_intersecting_neighbors = node.GetNumOfIntersectingNeighbors()
	if num_intersecting_neighbors <= 1: 
		return 
	
	# find the mapping between each split cluster and the x 
	cluster_elements = node.GetElements()
	intersection_union = set() 
	union_all = set(cluster_elements)
	num_intersecting_neighbors = 0
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements():
		if neighbor_id.startswith(next_timepoint): 
			num_intersecting_neighbors += 1
			neighbor_elements = transition_graph.GetNodeByID(neighbor_id).GetElements()
			intersection_union.update(intersection)
			union_all.update(set(neighbor_elements))
			
	if num_intersecting_neighbors <= 1:
		return 
	
	x = len(intersection_union) / len(union_all)

	return x 

def XMerge(node, transition_graph, timepoint, list_of_timepoint): 
	""" compute the merge core x between current timepoint and next timepoint 
		* return x value, if not None 
	"""
	# determine previous timepoint, if any 
	current_timepoint_idx = list_of_timepoint.index(timepoint)
	if current_timepoint_idx > 0:
		previous_timepoint = list_of_timepoint[current_timepoint_idx - 1]
	else: return
	
	# check the number of intersecting neighbors 
	num_intersecting_neighbors = node.GetNumOfIntersectingNeighbors()
	if num_intersecting_neighbors <= 1: 
		return 
	
	# find the mapping between each merged cluster and the x 
	cluster_elements = node.GetElements()
	intersection_union = set() 
	union_all = set(cluster_elements) 
	num_intersecting_neighbors = 0
	for neighbor_id, intersection in node.GetIntersectingNeighborsAndElements():
		if neighbor_id.startswith(previous_timepoint): 
			num_intersecting_neighbors += 1
			neighbor_elements = transition_graph.GetNodeByID(neighbor_id).GetElements()
			intersection_union.update(intersection)
			union_all.update(set(neighbor_elements))
	
	if num_intersecting_neighbors <= 1:
		return 
	
	x = len(intersection_union) / len(union_all)
	return x 
	
	
def SetFuzzyNodeDisappear(transition_graph, list_of_timepoint): 
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint: 
		for node in transition_graph.GetNodesAtTimepoint(timepoint): 
			x = XDisappear(node, transition_graph, timepoint, list_of_timepoint)
			node.SetDisappearStrength(x)

def SetFuzzyEdgeUnchanged(transition_graph, list_of_timepoint): 
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint[:-1]:
		for node in transition_graph.GetNodesAtTimepoint(timepoint):
			current_node_id = node.GetID()
			node_id_to_x_mapping = XUnchange(node, transition_graph, timepoint, list_of_timepoint)
			if node_id_to_x_mapping: 
				for neighbor_id, x in node_id_to_x_mapping.items(): 
					transition_graph.GetEdge(current_node_id, neighbor_id).AddFuzzyType('unchanged', x)
				
def SetFuzzyEdgeAbsorbed(transition_graph, list_of_timepoint): 
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint[:-1]:
		for node in transition_graph.GetNodesAtTimepoint(timepoint):
			current_node_id = node.GetID()
			node_id_to_x_mapping = XAbsorb(node, transition_graph, timepoint, list_of_timepoint)
			if node_id_to_x_mapping: 
				for neighbor_id, x in node_id_to_x_mapping.items(): 
					transition_graph.GetEdge(current_node_id, neighbor_id).AddFuzzyType('absorbed', x)
				
def SetFuzzyEdgeDissolve(transition_graph, list_of_timepoint): 
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint[:-1]:
		for node in transition_graph.GetNodesAtTimepoint(timepoint):
			current_node_id = node.GetID()
			node_id_to_x_mapping = XDissolve(node, transition_graph, timepoint, list_of_timepoint)
			if node_id_to_x_mapping: 
				for neighbor_id, x in node_id_to_x_mapping.items(): 
					transition_graph.GetEdge(current_node_id, neighbor_id).AddFuzzyType('dissolved', x)
				
def SetFuzzyEdgeSplit(transition_graph, list_of_timepoint):
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint[:-1]:
		for node in transition_graph.GetNodesAtTimepoint(timepoint):
			x = XSplit(node, transition_graph, timepoint, list_of_timepoint)
			if x:
				for neighbor_id, edge in node.GetOutgoingNeighborsAndEdges(): 
					edge.AddFuzzyType('split', x)

def SetFuzzyEdgeMerge(transition_graph, list_of_timepoint): 
	""" add the fuzzy edges and set type to transition graph """
	for timepoint in list_of_timepoint[1:]:
		for node in transition_graph.GetNodesAtTimepoint(timepoint):
			x = XMerge(node, transition_graph, timepoint, list_of_timepoint)
			if x:
				for neighbor_id, edge in node.GetIncomingNeighborsAndEdges(): 
					edge.AddFuzzyType('merged', x)
				
				
def MakeTransitionGraphFuzzy(list_of_timepoint, clustering_by_timepoint): 
	""" main function to create and setup fuzzy transition graph """
	transition_graph = MakeTransitionGraphRaw(list_of_timepoint, clustering_by_timepoint)
	SetFuzzyNodeDisappear(transition_graph, list_of_timepoint)
	SetFuzzyEdgeUnchanged(transition_graph, list_of_timepoint)
	SetFuzzyEdgeAbsorbed(transition_graph, list_of_timepoint)
	SetFuzzyEdgeDissolve(transition_graph, list_of_timepoint)
	SetFuzzyEdgeSplit(transition_graph, list_of_timepoint)
	SetFuzzyEdgeMerge(transition_graph, list_of_timepoint)
	
	return transition_graph 


def ComputeWeakMiu(x, a, b, c, d): 
	""" compute strong fuzzy sets given core x and limiter a, b, c, d"""
	if 0 <= x <= a:
		return 1
	elif a < x < b: 
		return (b-x)/(b-a)
	else: 
		return 
	
def ComputeMediumMiu(x, a, b, c, d): 
	""" compute medium fuzzy sets given core x and limiter a, b, c, d"""
	if a < x < b: 
		return (x-a)/(b-a)
	elif b <= x <= c: 
		return 1
	elif c < x < d: 
		return (d-x)/(d-c) 
	else:
		return 
	
def ComputeStrongMiu(x, a, b, c, d): 
	""" compute weak fuzzy sets given core x and limiter a, b, c, d"""
	if c < x < d: 
		return (x-c)/(d-c)
	elif x >= d: 
		return 1 
	else:
		return 

def ComputeFuzzySets(x, a, b, c, d): 
	""" main function to compute and return the fuzzy sets"""
	fuzzy_sets = list() 
	
	weak_x = ComputeWeakMiu(x, a, b, c, d)
	medium_x = ComputeMediumMiu(x, a, b, c, d)
	strong_x = ComputeStrongMiu(x, a, b, c, d)
	
	if weak_x: 
		fuzzy_sets.append(('weak', weak_x))
	if medium_x: 
		fuzzy_sets.append(('medium', medium_x))
	if strong_x: 
		fuzzy_sets.append(('strong', strong_x))
		
	return fuzzy_sets