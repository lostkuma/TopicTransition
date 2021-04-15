
# ClusterNode, GraphEdge, Graph 

class ClusterNode(object): 
	def __init__(self, cluster_elements, timepoint, cluster_idx): 
		self.cluster = set(cluster_elements) 
		self.size = len(self.cluster) 
		self.timepoint = timepoint 
		self.cluster_idx = cluster_idx 
		self.cluster_id = str(timepoint) + '_' + str(cluster_idx).zfill(2)
		self.incoming_neighbors = dict() # {node_id: edge_obj }
		self.outgoing_neighbors = dict() # {node_id: edge_obj }
		self.reappear_neighbors = dict() 
		self.node_id_to_inter_edge_mapping = dict() 
		self.x_disappear = None 

	def __str__(self): 
		return 'cluster {} at timepoint {}, number of elements in cluster: {}'.format(self.cluster_idx, self.timepoint, self.size)

	def GetElements(self): 
		return self.cluster 

	def GetID(self):
		return self.cluster_id 

	def GetTimepoint(self):
		return self.timepoint 

	def GetIndex(self): 
		return self.cluster_idx 

	def GetSize(self): 
		return self.size 

	def HasIncomingNeighbors(self): 
		if len(self.incoming_neighbors) > 0: 
			return True 
		return False 

	def HasOutgoingNeighbors(self):
		if len(self.outgoing_neighbors) > 0: 
			return True 
		return False 

	def HasReappearNeigbhors(self): 
		if len(self.reappear_neighbors) > 0: 
			return True 
		return False 
		
	def HasNeighbors(self, include_reappear=False): 
		if not include_reappear:
			if not self.HasIncomingNeighbors() and not self.HasOutgoingNeighbors(): 
				return False
		else: 
			if not self.HasIncomingNeighbors() and not self.HasOutgoingNeighbors() and not self.HasReappearNeigbhors():
				return False 
		return True 

	def GetIncomingNeighbors(self):
		if self.HasIncomingNeighbors():
			return list(self.incoming_neighbors.keys())

	def GetOutgoingNeighbors(self): 
		if self.HasOutgoingNeighbors(): 
			return list(self.outgoing_neighbors.keys()) 

	def GetReappearNeighbors(self): 
		if self.HasReappearNeigbhors(): 
			return list(self.reappear_neighbors.keys()) 
			
	def GetNeighborsAndEdges(self, include_reappear=False):
		for node_id, edge_obj in self.incoming_neighbors.items(): 
			yield node_id, edge_obj
				
		for node_id, edge_obj in self.outgoing_neighbors.items(): 
			yield node_id, edge_obj 
		
		if include_reappear: 
			for node_id, edge_obj in self.reappear_neighbors.items(): 
				yield node_id, edge_obj 

	def GetIncomingNeighborsAndEdges(self):
		for node_id, edge_obj in self.incoming_neighbors.items(): 
			yield node_id, edge_obj 

	def GetOutgoingNeighborsAndEdges(self): 
		for node_id, edge_obj in self.outgoing_neighbors.items(): 
			yield node_id, edge_obj 

	def GetReappearNeighborsAndEdges(self): 
		for node_id, edge_obj in self.reappear_neighbors.items(): 
			yield node_id, edge_obj 

	def HasEdge(self, neighbor_id, include_reappear=False): 
		if neighbor_id in self.incoming_neighbors: 
			return True
		if neighbor_id in self.outgoing_neighbors: 
			return True 
		if include_reappear and neighbor_id in self.reappear_neighbors:
			return True 
		return False 

	def GetEdge(self, neighbor_id, include_reappear=False): 
		if neighbor_id in self.incoming_neighbors: 
			return self.incoming_neighbors[neighbor_id] 
		if neighbor_id in self.outgoing_neighbors: 
			return self.outgoing_neighbors[neighbor_id] 
		if include_reappear and neighbor_id in self.reappear_neighbors: 
			return self.reappear_neighbors[neighbor_id] 

	def AddIncomingNeighbor(self, neighbor_id, edge):
		self.incoming_neighbors[neighbor_id] = edge 

	def AddOutgoingNeighbor(self, neighbor_id, edge):
		self.outgoing_neighbors[neighbor_id] = edge 
		
	def AddReappearNeighbor(self, node, type, element_change=0): 
		assert type != None
		neighbor_id = node.GetID() 
		edge = GraphEdge(self, node, type, element_change) 
		self.reappear_neighbors[neighbor_id] = edge 

	def AddIntersectingEdge(self, node_id, edge): 
		self.node_id_to_inter_edge_mapping[node_id] = edge 
		
	def GetIntersectingElements(self, node_id): 
		edge = self.node_id_to_inter_edge_mapping[node_id]
		return edge.GetIntersectingElements()
		
	def GetIntersectingNeighborsAndElements(self): 
		for node_id, edge in self.node_id_to_inter_edge_mapping.items(): 
			yield node_id, edge.GetIntersectingElements()
			
	def GetNumOfIntersectingNeighbors(self): 
		return len(self.node_id_to_inter_edge_mapping)
		
	def SetDisappearStrength(self, x): 
		self.x_disappear = x 
		
	def GetDisappearStrength(self):
		return self.x_disappear 


# directed edge goes from time_a to time_a+1
class GraphEdge(object): 
	def __init__(self, cluster_1, cluster_2, type=None, element_change=0): 
		self.start = cluster_1 # on the no arrow side 
		self.end = cluster_2 # on the arrow side 
		self.type = type 
		self.fuzzy_types = dict() # {type: x }
		self.element_change = element_change 

	def __str__(self): 
		return '{} --> {}, type {}, element change {}'.format(cluster_1.GetID(), cluster_2.GetID(), self.type, self.element_change)
		
	def SetEdgeType(self, type):
		self.type = type 
		
	def UpdateElementChangePercentage(self, val): 
		self.element_change = val 
		
	def GetEdgeType(self): 
		return self.type 

	def GetNodeStart(self):
		return self.start 
		
	def GetNodeEnd(self):
		return self.end 
		
	def GetElementChangePercentage(self):
		return self.element_change 
	
	def AddFuzzyType(self, type, x): 
		self.fuzzy_types[type] = x 
	
	def GetFuzzytypes(self):
		return self.fuzzy_types 


class GraphEdgeIntersection(object): 
	def __init__(self, node_id_1, node_id_2, intersection): 
		self.intersection = set(intersection) 
		self.node_1 = node_id_1 
		self.node_2 = node_id_2 
	
	def GetIntersectingElements(self): 
		return self.intersection 
		
	def GetNumOfIntersectingElements(self): 
		return len(self.intersection) 
		
	def GetNodeIDs(self): 
		return self.node_1, self.node_2 



class Graph(object): 
	def __init__(self): 
		self.cluster_id_to_node_mapping = dict() 
		self.timepoint_to_node_mapping = dict()
		self.node_pair_to_inter_edge_mapping = dict() #{(node_id_1, node_id_2)}

	# def __eq__(self, rhs)
	# https://stackoverflow.com/questions/390250/elegant-ways-to-support-equivalence-equality-in-python-classes 
	def __eq__(self, other):
		if isinstance(other, self.__class__):
			return set(self.GetAllNodesID()) == set(other.GetAllNodesID())
		else:
			return False
			
	def __hash__(self):
		return hash(set(self.GetAllNodesID()))

	def HasNode(self, node_id): 
		return node_id in self.cluster_id_to_node_mapping
	
	def GetNumOfNodes(self): 
		return len(self.cluster_id_to_node_mapping) 
		
	def GetNumOfNodesByTimepoint(self, timepoint): 
		return len(self.timepoint_to_node_mapping[timepoint])
	
	def GetAllNodes(self): 
		for node_id, node in self.cluster_id_to_node_mapping.items():
			yield node 

	def GetAllNodesID(self):
		return list(self.cluster_id_to_node_mapping.keys()) 

	def GetNodesAtTimepoint(self, timepoint): 
		for node in self.timepoint_to_node_mapping[timepoint]: 
			yield node 
			
	def GetSortedNodesAtTimepoint(self, timepoint): 
		ordered_nodes_id = list() 
		for nodes in self.GetNodesAtTimepoint(timepoint): 
			node_id = nodes.GetID()
			ordered_nodes_id.append(node_id) 
			
		ordered_nodes_id = sorted(ordered_nodes_id) 
		for node_id in ordered_nodes_id: 
			yield self.GetNodeByID(node_id)
	
	def GetNodeByID(self, node_id): 
		assert self.HasNode(node_id), 'node not in graph'
		return self.cluster_id_to_node_mapping[node_id]

	def GetNodeByTimepointAndIndex(self, timepoint, cluster_idx):
		node_id = str(timepoint) + '_' + str(cluster_idx).zfill(2) 
		assert self.HasNode(node_id), 'node not in graph'
		return self.cluster_id_to_node_mapping[node_id]

	def HasEdge(self, node_id_1, node_id_2, include_reappear=False): 
		node_1 = self.GetNodeByID(node_id_1)
		return node_1.HasEdge(node_id_2, include_reappear) 
		
	def GetEdge(self, node_id_1, node_id_2, include_reappear=False):
		node_1 = self.GetNodeByID(node_id_1) 
		return node_1.GetEdge(node_id_2, include_reappear) 
		
	def GetEdgeFuzzySets(self, node_id_1, node_id_2, include_reappear=False): 
		edge = self.GetEdge(node_id_1, node_id_2, include_reappear)
		return edge.GetFuzzytypes()
		
	def AddIntersectingEdge(self, node_id_1, node_id_2, intersection): 
		assert len(intersection) != 0, 'no intersecting elements'
		edge = GraphEdgeIntersection(node_id_1, node_id_2, intersection) 
		self.node_pair_to_inter_edge_mapping[(node_id_1, node_id_2)] = edge 
		self.GetNodeByID(node_id_1).AddIntersectingEdge(node_id_2, edge)
		self.GetNodeByID(node_id_2).AddIntersectingEdge(node_id_1, edge)

	def HasIntersectingEdge(self, node_id_1, node_id_2): 
		return (node_id_1, node_id_2) in self.node_pair_to_inter_edge_mapping 

	def GetIntersectingEdge(self, node_id_1, node_id_2): 
		return self.node_pair_to_inter_edge_mapping[(node_id_1, node_id_2)]
		
	def GetIntersectingElements(self, node_id_1, node_id_2): 
		return self.GetIntersectingEdge(node_id_1, node_id_2).GetIntersectingElements()
	
	def AddNode(self, cluster_elements, timepoint, cluster_idx):
		node = ClusterNode(cluster_elements, timepoint, cluster_idx)
		node_id = node.GetID()
		self.cluster_id_to_node_mapping[node_id] = node 
		
		if timepoint not in self.timepoint_to_node_mapping:
			self.timepoint_to_node_mapping[timepoint] = list() 
		
		self.timepoint_to_node_mapping[timepoint].append(node) 
		
	def AddDirectedEdge(self, cluster_1, cluster_2, type, element_change=0):
		cluster_id_1 = cluster_1.GetID()
		cluster_id_2 = cluster_2.GetID()
		assert self.HasNode(cluster_id_1) and self.HasNode(cluster_id_2) 
		assert type != 'reappear'
		edge = GraphEdge(cluster_1, cluster_2, type, element_change)
		cluster_1.AddOutgoingNeighbor(cluster_id_2, edge) 
		cluster_2.AddIncomingNeighbor(cluster_id_1, edge)

	def AddReappearEdge(self, cluster_1, cluster_2, element_change=0): 
		cluster_id_1 = cluster_1.GetID()
		cluster_id_2 = cluster_2.GetID() 
		assert self.HasNode(cluster_id_1) and self.HasNode(cluster_id_2) 
		cluster_1.AddReappearNeighbor(cluster_2, 'reappear', element_change) 
		cluster_2.AddReappearNeighbor(cluster_1, 'reappear', element_change * -1)
		
	def AddEdge(self, cluster_1, cluster_2, type, element_change=0):
		if type == 'reappear':
			self.AddReappearEdge(cluster_1, cluster_2, element_change)
		else:
			self.AddDirectedEdge(cluster_1, cluster_2, type, element_change)

	def GetTransitionType(self, node_id_1, node_id_2, include_reappear=False): 
		if self.HasEdge(node_id_1, node_id_2, include_reappear):
			node_1 = self.GetNodeByID(node_id_1)
			node_2 = self.GetNodeByID(node_id_2)
			type = self.GetEdge(node_id_1, node_id_2, include_reappear).GetEdgeType() 
			return type
		assert False, "No edge between " + node_id_1 + " and " + node_id_2

	def GetTransitionSubgraphByNodeID(self, node_id, include_reappear=False): 
		# get a subgraph with the starting node 
		subgraph = Graph() 
		node_id_to_visit = [node_id] 
		nodes_visited = list() 
		
		while len(node_id_to_visit) > 0: 
			
			current_node_id = node_id_to_visit.pop(0) 
			
			if current_node_id not in nodes_visited: 
				current_node = self.GetNodeByID(current_node_id) 
				current_elements = current_node.GetElements() 
				current_timepoint = current_node.GetTimepoint()
				current_index = current_node.GetIndex()
				
				if not subgraph.HasNode(current_node_id):
					subgraph.AddNode(current_elements, current_timepoint, current_index)
					
				for neighbor_id, edge_obj in current_node.GetNeighborsAndEdges(include_reappear):
					node_id_to_visit.append(neighbor_id) 

					neighbor_node = self.GetNodeByID(neighbor_id)
					neighbor_elements = neighbor_node.GetElements() 
					neighbor_timepoint = neighbor_node.GetTimepoint() 
					neighbor_iddex = neighbor_node.GetIndex() 

					if not subgraph.HasNode(neighbor_id):
						subgraph.AddNode(neighbor_elements, neighbor_timepoint, neighbor_iddex)

					edge_type = edge_obj.GetEdgeType() 
					edge_element_change = edge_obj.GetElementChangePercentage() 
					
					node_a = subgraph.GetNodeByID(edge_obj.GetNodeStart().GetID())
					node_b = subgraph.GetNodeByID(edge_obj.GetNodeEnd().GetID())
					
					subgraph.AddEdge(node_a, node_b, edge_type, edge_element_change) 

				nodes_visited.append(current_node_id) 
		
		return subgraph 
		
	def PrintTransitionGraph(self, include_reappear=True): 
		nodes_id_in_graph = self.GetAllNodesID()
		nodes_id_in_graph = sorted(nodes_id_in_graph)
		
		print('TransitionGraph')
		for node_id in nodes_id_in_graph:
			node = self.GetNodeByID(node_id)
			for neighbor_node_id, edge in node.GetNeighborsAndEdges(include_reappear):
				if node_id < neighbor_node_id:
					print(node_id, '--', edge.GetEdgeType(), '-->', neighbor_node_id)
			
 
