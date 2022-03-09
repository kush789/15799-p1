import json

def task_project1_setup():

    return {
        "actions": [
            'sudo apt-get update', 
            'sudo apt-get -y install python3-pip', 
            'sudo pip3 install pandas', 
            'sudo pip3 install sqlparse', 
            'sudo pip3 install sql-metadata'
        ]
    }

def task_project1():

    def generate_actions_file(workload_csv):
        if workload_csv == "":
            print ("====================>>>>> workload_csv is default!!")
        else:
            print ("====================>>>>> workload_csv is ", workload_csv)
        with open("actions.sql", "w") as fp:
            fp.write("SELECT 1;")

        with open("config.json", "w") as fp:
            fp.write('{"VACUUM": true}')

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [(generate_actions_file,)],
        "params": [{
            "name": "workload_csv",
            "long": "workload_csv",
            "default": "",

        }],
        # Always rerun this task.
        "uptodate": [False],
    }
