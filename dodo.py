def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
    }
