# crisp pairwise clustering transition code 
# given cluster i (current_cluster) of clustering a at time t_a, trace it in clustering b at time t_a+1
# FindMatchingClustersMain, FindMatchingClustersMainSingleCluster, MatchReappearingClusters

def _getCurrentClusterDistribution(current_cluster, clustering_b): 
	""" given current cluster and the clustering_b from a different day, 
		return a list of tokens that do not present in clustering_b,
		return a dictionary for tokens mapping from current cluster to clustering_b 
	"""
	current_cluster_size = len(current_cluster)
	current_cluster_to_b_mapping = dict(zip(current_cluster, [None for i in range(len(current_cluster))]))

	# add elements from current_cluster to mapping if it appears in clustering_b
	for cl_idx in range(len(clustering_b)):
		for element in clustering_b[cl_idx]: 
			if element in current_cluster_to_b_mapping: 
				current_cluster_to_b_mapping[element] = cl_idx 

	# flip the dictionary (filter out None vals)
	elements_not_passed = list()
	current_cluster_split_mapping = dict()
	for token, cl_idx in current_cluster_to_b_mapping.items(): 
		if cl_idx == None: 
			elements_not_passed.append(token)
		else: 
			if not cl_idx in current_cluster_split_mapping: 
				current_cluster_split_mapping[cl_idx] = list() 
			current_cluster_split_mapping[cl_idx].append(token) 

	return elements_not_passed, current_cluster_split_mapping 
	

def _ifClusterDisappeared(current_cluster, elements_not_passed, threshold):
	""" check if a cluster disappears or not given a threshold"""
	if len(elements_not_passed) / len(current_cluster) > threshold: 
		return True

	return False
	

def _findUnchangedClusters(current_cluster, clustering_b, current_cluster_to_b_mapping, threshold_passed, threshold_criteria):
	""" find unchanged cluster given current cluster, clustering_b, mapping, and thresholds, 
		find exactly one cluster in clustering_b that matches with current cluster,
		* threshold_passed - for how much percentage elements are passed to clustering_b 
		* threshold_criteria - for how much percentage elements make up the corresponding cluster in clustering_b 
		* return matching cluster index in clustering_b 
		
		** need to assume _ifClusterDisappeared is False
	"""
	num_corresponding_b = 0
	size_current_cluster = len(current_cluster)
	cluster_b_index = None

	for cl_idx_b, corresponding_b in current_cluster_to_b_mapping.items(): 
		size_b_actual = len(clustering_b[cl_idx_b])
		size_corresponding_b = len(corresponding_b)

		if size_corresponding_b / size_b_actual >= threshold_criteria and size_corresponding_b / size_current_cluster >= threshold_passed: 
			num_corresponding_b += 1 
			cluster_b_index = cl_idx_b 

	if num_corresponding_b == 1: 
		return cluster_b_index 

	return False 


def _findSplitClusters(current_cluster, clustering_b, current_cluster_to_b_mapping, threshold_passed, threshold_criteria): 
	""" find split cluster given current cluster, clustering_b, mapping, and thresholds, 
		find multiple clusters in clustering b and the union of them makes up current cluster, 
		* threshold_passed - for how much percentage elements are passed to clustering_b 
		* threshold_criteria - for how much percentage elements make up the corresponding clusters in clustering_b 
		* return a list of matching cluster indices in clustering_b 
		
		** need to assume _ifClusterDisappeared is False
	"""
	num_corresponding_b = 0
	sum_size_corresponding_b = 0
	size_current_cluster = len(current_cluster)
	cluster_b_indices = list() 

	for cl_idx_b, corresponding_b in current_cluster_to_b_mapping.items(): 
		size_b_actual = len(clustering_b[cl_idx_b])
		size_corresponding_b = len(corresponding_b)

		if size_corresponding_b / size_b_actual >= threshold_criteria: 
			num_corresponding_b += 1 
			sum_size_corresponding_b += size_corresponding_b
			cluster_b_indices.append(cl_idx_b)
			
	# if there are multiple cluster in clustering b and the sum of them makes up current cluster within threshold
	if num_corresponding_b > 1 and sum_size_corresponding_b / size_current_cluster >= threshold_passed: 
		return cluster_b_indices

	return False 
	
	
def _findAbsorbedClusters(current_cluster, current_cluster_to_b_mapping, threshold_passed, threshold_criteria): 
	""" find abosorbed cluster given current cluster, clustering_b, mapping, and thresholds, 
		find one cluster that contains more than threshold percentage of the current cluster passed,
		* threshold_passed - for how much percentage elements are passed to clustering_b 
		* threshold_criteria - for how much percentage elements make up the corresponding clusters in clustering_b 
		* return matching cluster index in clustering_b 
		
		** need to assume _ifClusterDisappeared is False
		** need to assume _ifClusterUnchanged is False
		** need to assume _ifClusterSplit is False
	"""
	num_corresponding_b = 0	   
	sum_size_corresponding_b = 0
	size_largest_corresponding_b = 0
	size_current_cluster = len(current_cluster)
	cluster_b_index = None 

	for cl_idx_b, corresponding_b in current_cluster_to_b_mapping.items(): 
		size_corresponding_b = len(corresponding_b)
		sum_size_corresponding_b += size_corresponding_b
		num_corresponding_b += 1

		if size_largest_corresponding_b < size_corresponding_b: 
			size_largest_corresponding_b = size_corresponding_b 
			cluster_b_index = cl_idx_b 

	# if the largest corresponding b contains more than threshold number of elements from all clusters in clustering b
	# also the largest corresponding b needs to contain more than threshold number of elements from current cluster in clustering a
	
	if num_corresponding_b > 0 and size_largest_corresponding_b / sum_size_corresponding_b >= threshold_criteria and size_largest_corresponding_b / size_current_cluster >= threshold_passed: 
		return cluster_b_index
	
	return False 
	
	
def FindMatchingClustersForward(clustering_a, clustering_b, threshold_passed=2/3, threshold_criteria=2/3):
	""" find crisp matching cluster for forward direction 
		follow the following ordering of checks
			- if disappear
			- if unchagned 
			- if split 
			- if absorbed 
		return found matchings from clustering_a to clustering_b: [dict, dict, dict], empty dict if no matching 
	"""
	unchanged_matching = dict()
	absorbed_matching = dict()
	split_matching = dict()
	
	for i in range(len(clustering_a)): 
		current_cluster = clustering_a[i]
		elements_not_passed, current_cluster_to_b_mapping = _getCurrentClusterDistribution(current_cluster, clustering_b)
		
		if not _ifClusterDisappeared(current_cluster, elements_not_passed, threshold_passed): 
			
			unchanged_cluster_idx = _findUnchangedClusters(current_cluster, clustering_b, current_cluster_to_b_mapping, threshold_passed, threshold_criteria)
			
			split_cluster_idx = _findSplitClusters(current_cluster, clustering_b, current_cluster_to_b_mapping, threshold_passed, threshold_criteria)
			
			if unchanged_cluster_idx: 
				unchanged_matching[i] = unchanged_cluster_idx
			
			elif split_cluster_idx: 
				split_matching[i] = split_cluster_idx
				
			else:
				absorbed_cluster_idx = _findAbsorbedClusters(current_cluster, current_cluster_to_b_mapping, threshold_passed, threshold_criteria)
				if absorbed_cluster_idx: 
					absorbed_matching[i] = absorbed_cluster_idx
				
	return unchanged_matching, absorbed_matching, split_matching
	
	
def FindMatchingClustersBackward(clustering_a, clustering_b, threshold_passed=2/3, threshold_criteria=2/3):
	""" swap the input of clustering_a and clustering_b to find the matching backwards, 
		* return backwards types include: 
			- unchanged, dissolved, merged [dict, dict, dict], empty dict if no matching 
	"""
	unchanged_matching, dissolved_matching, merged_matching = FindMatchingClustersForward(clustering_b, clustering_a, threshold_passed, threshold_criteria)
				
	return unchanged_matching, dissolved_matching, merged_matching
	

def FindMatchingClustersMain(clustering_a, clustering_b, threshold_passed=2/3, threshold_criteria=2/3): 
	""" main function for find all pairwise transition types for going from clustering_a to clustering_b 
		* return dict of dict for types, empty dict if no matching 
	"""
	dissolved_matching_forward = dict()
	merged_matching_forward = dict()
	
	unchanged_matching, absorbed_matching, split_matching = FindMatchingClustersForward(clustering_a, clustering_b, threshold_passed, threshold_criteria)
	_, dissolved_matching, merged_matching = FindMatchingClustersBackward(clustering_a, clustering_b, threshold_passed, threshold_criteria)
	
	# flip the dissolved matching dictionary 
	if len(dissolved_matching) > 0:
		for cluster_b_idx, cluster_a_idx in dissolved_matching.items(): 
			dissolved_matching_forward[cluster_a_idx] = cluster_b_idx 
	
	# flip the merged_matching dictionary 
	if len(merged_matching) > 0: 
		for cluster_b_idx, cluster_a_indices in merged_matching.items(): 
			cluster_a_indices = tuple(cluster_a_indices)
			merged_matching_forward[cluster_a_indices] = cluster_b_idx 
	
	output = {'unchanged': unchanged_matching,
			  'absorbed': absorbed_matching, 
			  'split': split_matching, 
			  'dissolved': dissolved_matching_forward, 
			  'merged': merged_matching_forward
			  }
	
	return output


def FindMatchingClustersMainSingleCluster(clustering_a, current_cluster_idx, clustering_b, threshold_passed=2/3, threshold_criteria=2/3): 
	""" A variation of the FindMatchingClustersMain function, which return the output for a single cluster in clustering_a 
		* return dict of types, None if no matching found 
	"""
	dissolved_matching_forward = dict()
	merged_matching_forward = dict()
	output = {'unchanged': None,
		  'absorbed': None, 
		  'split': None, 
		  'dissolved': None, 
		  'merged': None
		  }
	
	unchanged_matching, absorbed_matching, split_matching = FindMatchingClustersForward(clustering_a, clustering_b, threshold_passed, threshold_criteria)
	_, dissolved_matching, merged_matching = FindMatchingClustersBackward(clustering_a, clustering_b, threshold_passed, threshold_criteria)
	
	# flip the dissolved matching dictionary 
	if len(dissolved_matching) > 0:
		for cluster_b_idx, cluster_a_idx in dissolved_matching.items(): 
			dissolved_matching_forward[cluster_a_idx] = cluster_b_idx 
	
	# flip the merged_matching dictionary 
	if len(merged_matching) > 0: 
		for cluster_b_idx, cluster_a_indices in merged_matching.items(): 
			cluster_a_indices = tuple(cluster_a_indices)
			merged_matching_forward[cluster_a_indces] = cluster_b_idx 
			
	if current_cluster_idx in unchanged_matching: 
		output['unchanged'] = unchanged_matching[current_cluster_idx]
	if current_cluster_idx in absorbed_matching: 
		output['absorbed'] = absorbed_matching[current_cluster_idx]
	if current_cluster_idx in split_matching: 
		output['split'] = split_matching[current_cluster_idx]
	if current_cluster_idx in dissolved_matching: 
		output['dissolved'] = dissolved_matching[current_cluster_idx]
	if current_cluster_idx in merged_matching: 
		output['merged'] = merged_matching[current_cluster_idx]
	
	return output
	

def MatchReappearingClusters(current_cluster, clustering_b, threshold_passed=1/2, threshold_criteria=1/2): 
	""" function to match reappearing clusters, only check for unchanged 
		* return the index of the matching clustering in clustering_b 
	"""
	unchanged_cluster_idx = None 
	elements_not_passed, current_cluster_to_b_mapping = _getCurrentClusterDistribution(current_cluster, clustering_b) 
	
	if not _ifClusterDisappeared(current_cluster, elements_not_passed, threshold_passed): 
		unchanged_cluster_idx = _findUnchangedClusters(current_cluster, clustering_b, current_cluster_to_b_mapping, threshold_passed, threshold_criteria)
		
	return unchanged_cluster_idx
	
	