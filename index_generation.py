from collections import defaultdict

def generate_all_indexes(where_predicates):

	index_set = defaultdict(lambda : defaultdict(lambda : 0))

	for predicate, count in sorted(where_predicates.items(), key = lambda x : -x[1]):

		table_column_usage = defaultdict(lambda : [])

		for left, right in predicate:

			table, column = left.split(".")
			if column not in table_column_usage[table]:
				table_column_usage[table].append(column)

			if right is None:
				continue

			table, column = right.split(".")
			if column not in table_column_usage[table]:
				table_column_usage[table].append(column)

		for table, columns in table_column_usage.items():
			index_set[table][tuple(sorted(columns))] += count
			
	return index_set

def prune_indexes(index_set):

	_pruned_index_set = defaultdict(lambda : [])

	for table, indexes in index_set.items():
		for index, count in indexes.items():
			_pruned_index_set[table].append( [", ".join(list(index)), count] )
		_pruned_index_set[table] = sorted(_pruned_index_set[table], key = lambda x : len(x[0]))

	pruned_index_set = defaultdict(lambda : [])

	for table, indexes_and_count in _pruned_index_set.items():
		

		for i in range(len(indexes_and_count)):

			index, count = indexes_and_count[i]

			add_index_to_pruned_set = True

			for j in range(i + 1, len(indexes_and_count)):
				if indexes_and_count[j][0].startswith(indexes_and_count[i][0]):

					add_index_to_pruned_set = False
					indexes_and_count[j][1] += count
					break
			
			if add_index_to_pruned_set:
				pruned_index_set[table].append((indexes_and_count[i][0].split(", "), indexes_and_count[i][1]))
	
	return pruned_index_set

def select_top_indexes(index_set, threshold = 0.01):
	top_indexes = defaultdict(lambda : [])

	for table, cols_and_counts in index_set.items():
		total_counts = sum(map(lambda x : x[1], cols_and_counts))
		for cols, count in cols_and_counts:
			usage = count / total_counts
			if usage >= threshold:
				top_indexes[table].append((cols, count, usage))

	return top_indexes

def generate_create_index_commands(index_set):

	generated_commands = []

	for table, indexes in index_set.items():
		for (index, count, _) in indexes:
			create_index_command = "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);" % (
						table, 
						"_".join(list(index)), 
						table,
						", ".join(list(index)))
			generated_commands.append(create_index_command)
	return generated_commands
