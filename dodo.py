from doit import task_params

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

@task_params([{"name": "workload_csv", "default": "", "type": str, "long": "workload_csv"}])
def task_project1():

    print ("====================>>>>> workload_csv is " + workload_csv)

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
    }
