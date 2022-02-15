def task_project1():
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "CREATE INDEX idx_rating_uid ON review (u_id);" > actions.sql',
            'echo "CREATE INDEX idx_rating_aid ON review (a_id);" >> actions.sql',
            'echo "CREATE INDEX idx_rating_iid ON review (i_id);" >> actions.sql',
            'echo "CREATE INDEX idx_review_rating_uid ON review_rating (u_id);" >> actions.sql',
            'echo "CREATE INDEX idx_review_rating_aid ON review_rating (a_id);" >> actions.sql',
            'echo "CREATE INDEX idx_trust_sid ON trust (source_u_id);" >> actions.sql',
            'echo "CREATE INDEX idx_trust_tid ON trust (target_u_id);" >> actions.sql',
            'echo \'{"VACUUM": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
    }
