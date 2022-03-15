



def task_project1_setup():
    return {
        "actions": [

            # Env setup actions
            'sudo apt-get update', 
            'sudo apt-get -y install python3-pip', 
            'sudo pip3 install sqlparse', 
            'sudo pip3 install sql-metadata',
            'sudo pip3 install pprintpp',
            'sudo bash drop_existing_indices.sh',
        ],
        "verbosity": 2
    }

def task_project1():

    def generate_actions_file(workload_csv, timeout):

        import json
        import pprint
        import csv
        import pandas as pd

        from column_usage import parse_simple_logs
        from index_generation import generate_all_indexes, prune_indexes, generate_create_index_commands
        
        with open(workload_csv) as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            log_file_data = list(map(lambda x : x[13], reader))

        parsing_success, where_predicates = parse_simple_logs(log_file_data)

        print ("\n\n\n")
        print ("<<<<============== parsing_success ==============>>>>")
        pprint.pprint (parsing_success)

        print ("\n\n\n")
        print ("<<<<============== where_predicates ==============>>>>")
        pprint.pprint (where_predicates)

        all_indexes = generate_all_indexes(where_predicates)
        print ("\n\n\n")
        print ("<<<<============== all_indexes ==============>>>>")
        pprint.pprint (all_indexes)

        pruned_indexes = prune_indexes(all_indexes)
        print ("\n\n\n")
        print ("<<<<============== pruned_indexes ==============>>>>")
        pprint.pprint (pruned_indexes)

        index_commands = generate_create_index_commands(pruned_indexes)
        print ("\n\n\n")
        print ("<<<<============== pruned_indexes ==============>>>>")
        pprint.pprint (index_commands)
        
        with open("actions.sql", "w") as fp:
            for command in index_commands[:4]:
                fp.write("%s\n" % (command))

        with open("config.json", "w") as fp:
            fp.write('{"VACUUM": true}')

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [(generate_actions_file,)],
        "params": [{
            "name": "workload_csv",
            "long": "workload_csv",
            "default": "",
        }, {
            "name": "timeout",
            "long": "timeout",
            "default": "10m",

        }],
        # Always rerun this task.
        "uptodate": [False],
        "verbosity": 2
    }
