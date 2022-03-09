from collections import defaultdict

def generate_all_indexes(operator_column_usage, where_predicates):

    index_set = defaultdict(lambda : set())
    
    # TODO: This is broken; parsing doesn't give order by table information
#     if "order_by" in operator_column_usage:
#         for column, usage in sorted(operator_column_usage["order_by"].items(), key = lambda x : -x[1]):
#             index_set.append([column])
           
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
            index_set[table].add(tuple(sorted(columns)))

    return index_set
            
def prune_indexes(index_set):

    _pruned_index_set = defaultdict(lambda : [])

    for table, indexes in index_set.items():
        for index in indexes:
            _pruned_index_set[table].append(", ".join(list(index)))
        _pruned_index_set[table] = sorted(_pruned_index_set[table], key = lambda x : len(x))

    pruned_index_set = defaultdict(lambda : [])

    for table, indexes in _pruned_index_set.items():
        
        for i in range(len(indexes)):
            add_index_to_pruned_set = True

            for j in range(i + 1, len(indexes)):
                if indexes[j].startswith(indexes[i]):
                    add_index_to_pruned_set = False
                    break
            
            if add_index_to_pruned_set:
                pruned_index_set[table].append(indexes[i].split(", "))
    
    return pruned_index_set
    
def generate_create_index_commands(index_set):

    generated_commands = []

    for table, indexes in index_set.items():
        for index in indexes:
            create_index_command = "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);" % (
                        table, 
                        "_".join(list(index)), 
                        table,
                        ", ".join(list(index)))
            generated_commands.append(create_index_command)
    return generated_commands
