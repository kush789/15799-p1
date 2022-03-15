



def task_project1_setup():
    return {
        "actions": [

            # Env setup actions
            'sudo apt-get update', 
            'sudo apt-get -y install python3-pip', 
            'sudo pip3 install sqlparse', 
            'sudo pip3 install sql-metadata',
            'sudo pip3 install pprintpp',

            # Figure out existing indexes without constraints
            '''
            echo "select idx.relname as index_name
                    from pg_index pgi
                    join pg_class idx on idx.oid = pgi.indexrelid
                    join pg_namespace insp on insp.oid = idx.relnamespace
                    join pg_class tbl on tbl.oid = pgi.indrelid
                    join pg_namespace tnsp on tnsp.oid = tbl.relnamespace
                    join pg_indexes pgis on pgis.indexname = idx.relname
                    where not pgi.indisunique and 
                            not pgi.indisprimary and 
                            not pgi.indisexclusion 
                            and tnsp.nspname = 'public';" \
            | sudo -u postgres psql project1db  \
            | tail -n +3 \
            | head -n -2 \
            | awk '{ print "DROP INDEX" $0 ";"}' \
            > drop_existing_indices.sql;
            ''',

            # Just for debug
            'cat drop_existing_indices.sql;',

            # drop all existing indices without constraints
            'cat drop_existing_indices.sql | sudo -u postgres psql project1db;',
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
            for command in index_commands:
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
