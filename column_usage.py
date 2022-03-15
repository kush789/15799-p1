import pandas as pd
import sqlparse
import re
from collections import defaultdict
from sql_metadata import Parser
import pprint

def get_column_usage_from_logs(log_file_dataframe):

    valid_log_types = { "SELECT": True, "UPDATE": True }
    execute_unnamed_start_token = "execute <unnamed>:"
    execute_unnamed_start_token_len = len(execute_unnamed_start_token)
    transaction_regex = r"(execute S_[0-9]+: )(.*)"

    # Maintain parsing counters
    parsing_success = { 
        "success": 0,
        "success_log_type": defaultdict(lambda : 0),
        "failure": 0,
        "skipped": 0,
        "skipped_log_type": defaultdict(lambda : 0)
    }

    column_usage = defaultdict(lambda : defaultdict(lambda : 0))

    print ("Total log lines: ", len(log_file_dataframe))

    # Parse each log line
    log_count = 0
    for log_type, log in log_file_dataframe[[7, 13]].values:
        log_count += 1

        if log_count % 10000 == 0:
            print ("Processed log_count: ", log_count)
            pprint.pprint(parsing_success)
            print ("\n\n")

        # Ignore queries from log that are [ BEGIN, COMMIT ]
        if log_type not in valid_log_types:
            # Update counters
            parsing_success["skipped"] += 1
            parsing_success["skipped_log_type"][log_type] += 1
            continue

        if log.startswith(execute_unnamed_start_token):
            # Log line starts with "execute <unnamed>:"
            query = log[execute_unnamed_start_token_len:]
        else:
            # Log line starts with "execute S_<>"
            query = re.sub(transaction_regex, r"\2", log)

        try:
            # Update counters
            parsing_success["success"] += 1
            parsing_success["success_log_type"][log_type] += 1

            parsed_query = Parser(query)
            columns = parsed_query.columns_dict
            # Ignore pg system queries (touches any table that starts with "pq_")
            if any(map(lambda x : x.startswith("pg_"), parsed_query.tables)):
                continue

            # If only one table, prefix is not there; append
            table_prefix = ""
            if len(parsed_query.tables) == 1:
                table_prefix = parsed_query.tables[0] + "."

            for operator_type in columns:
                for column in columns[operator_type]:
                    if "." in column:
                        column_usage[operator_type][column] += 1
                    else:
                        column_usage[operator_type][table_prefix + column] += 1
        except Exception as e:
            parsing_success["failure"] += 1
        

    return parsing_success, column_usage

def get_where_usage(log_file_dataframe):

    valid_log_types = { "SELECT": True, "UPDATE": True }
    execute_unnamed_start_token = "execute <unnamed>:"
    execute_unnamed_start_token_len = len(execute_unnamed_start_token)
    transaction_regex = r"(execute S_[0-9]+: )(.*)"

    # Maintain parsing counters
    parsing_success = { 
        "success": 0,
        "success_log_type": defaultdict(lambda : 0),
        "failure": 0,
        "skipped": 0,
        "skipped_log_type": defaultdict(lambda : 0)
    }

    column_usage = defaultdict(lambda : defaultdict(lambda : 0))
    where_operator_usage = defaultdict(lambda : 0)

    print ("Total log lines: ", len(log_file_dataframe))

    # Parse each log line
    log_count = 0
    for log_type, log in log_file_dataframe[[7, 13]].values:
        log_count += 1

        if log_count % 10000 == 0:
            print ("Processed log_count: ", log_count)
            pprint.pprint(parsing_success)
            print ("\n\n")

        # Ignore queries from log that are [ BEGIN, COMMIT ]
        if log_type not in valid_log_types:
            # Update counters
            parsing_success["skipped"] += 1
            parsing_success["skipped_log_type"][log_type] += 1
            continue

        if log.startswith(execute_unnamed_start_token):
            # Log line starts with "execute <unnamed>:"
            query = log[execute_unnamed_start_token_len:]
        else:
            # Log line starts with "execute S_<>"
            query = re.sub(transaction_regex, r"\2", log)

        try:
            
            statements = sqlparse.parse(query)

            """ {'r': 'review', 't': 'trust'} """
            parsed_query = Parser(query)
            table_aliases = parsed_query.tables_aliases
            columns = parsed_query.columns_dict
            # Ignore pg system queries (touches any table that starts with "pq_")
            if any(map(lambda x : x.startswith("pg_"), parsed_query.tables)):
                continue

            for token in statements[0]:
                """
                    token type examples:
                        - sqlparse.sql.Where
                        - sqlparse.sql.From
                """


                """ Ignore non where clauses """
                if type(token) != sqlparse.sql.Where:
                    continue

                """
                    Each subtoken is a where condition; 
                    sublists contain individual tokens.
                    e.g.:
                        r.u_id=t.target_u_id: [<Identifier 'r.u_id' at ...>,  <Identifier 't.targ...' at ...>]
                """
                for sub_token in token.get_sublists():

                    # Get identifiers from sub_token
                    identifiers = list(sub_token.get_sublists())

                    """
                        Could be comparing two columns, or with a constant
                        Mark right as None if constant
                    """
                    if len(identifiers) == 2:
                        left, right = list(map(str, identifiers))
                    else:
                        left, right = str(identifiers[0]), None

                    """
                        We've identified columns, let's expand them if table is aliased
                        r.u_id -> review.u_id
                    """
                    left_full, right_full = left, right
                    
                    for alias, table_name in table_aliases.items():
                        if left.startswith(alias + "."):
                            left_full = table_name + "." + left.split(".")[1]
                        if right is not None and right.startswith(alias + "."):
                            right_full = table_name + "." + right.split(".")[1]
                            
                    """
                        If there is only one table, prefix it as is to column
                    """
                    if len(parsed_query.tables) == 1:
                        if "." not in left_full:
                            left_full = parsed_query.tables[0] + "." + left_full
                        if right_full is not None and "." not in right_full:
                            right_full = parsed_query.tables[0] + "." + right_full

                    where_operator_usage[(left_full, right_full)] += 1

        except Exception as e:
            parsing_success["failure"] += 1
        

    return parsing_success, column_usage, where_operator_usage



















def get_where_predicates_from_query(query):

    predicates = []
    
    statements = sqlparse.parse(query)

    """ {'r': 'review', 't': 'trust'} """
    parsed_query = Parser(query)
    table_aliases = parsed_query.tables_aliases
    columns = parsed_query.columns_dict
    tables = parsed_query.tables
    
    # Ignore pg system queries (touches any table that starts with "pq_")
    if any(map(lambda x : x.startswith("pg_"), tables)):
        return predicates

    for token in statements[0]:
        """
            token type examples:
                - sqlparse.sql.Where
                - sqlparse.sql.From
        """


        """ Ignore non where clauses """
        if type(token) != sqlparse.sql.Where:
            continue

        """
            Each subtoken is a where condition; 
            sublists contain individual tokens.
            e.g.:
                r.u_id=t.target_u_id: [<Identifier 'r.u_id' at ...>,  <Identifier 't.targ...' at ...>]
        """
        for sub_token in token.get_sublists():

            # Get identifiers from sub_token
            identifiers = list(sub_token.get_sublists())

            """
                Could be comparing two columns, or with a constant
                Mark right as None if constant
            """
            left = str(identifiers[0])
            if len(identifiers) > 1 and type(identifiers[1]) == sqlparse.sql.Identifier:
                right = str(identifiers[1])
            else:
                right = None

            """
                We've identified columns, let's expand them if table is aliased
                r.u_id -> review.u_id
            """
            left_full, right_full = left, right

            for alias, table_name in table_aliases.items():
                if left.startswith(alias + "."):
                    left_full = table_name + "." + left.split(".")[1]
                if right is not None and right.startswith(alias + "."):
                    right_full = table_name + "." + right.split(".")[1]

            """
                If there is only one table, prefix it as is to column
            """
            if len(parsed_query.tables) == 1:
                if "." not in left_full:
                    left_full = parsed_query.tables[0] + "." + left_full
                if right_full is not None and "." not in right_full:
                    right_full = parsed_query.tables[0] + "." + right_full
            
            predicates.append((left_full, right_full))

    return predicates



def parse_simple_logs(log_file_data):

    simple_log_prefix = "statement: "
    simple_log_prefix_len = len(simple_log_prefix)

    # Maintain parsing counters
    parsing_success = { 
        "success": 0,
        "success_log_type": defaultdict(lambda : 0),
        "failure": 0,
        "failure_reason": defaultdict(lambda : 0),
        "skipped": 0,
        "skipped_log_type": defaultdict(lambda : 0)
    }

    column_usage = defaultdict(lambda : defaultdict(lambda : 0))
    where_operator_usage = defaultdict(lambda : 0)

    print ("Total log lines: ", len(log_file_data))

    # Parse each log line
    log_count = 0
    for log in log_file_data:
        log_count += 1

        if log_count % 10000 == 0:
            print ("Processed log_count: ", log_count)
            pprint.pprint(parsing_success)
            print ("\n\n")

            continue


        # Ignore logs that are not statements
        if not log.startswith("statement") or \
        log.startswith("statement: BEGIN") or \
        log.startswith("statement: COMMIT") or \
        log.startswith("statement: SHOW") or \
        log.startswith("statement: SET"):
            # Update counters
            parsing_success["skipped"] += 1
            continue

        query = log[simple_log_prefix_len:]

        # Empty statements
        if len(query) < 5:
            parsing_success["skipped"] += 1        
            continue

        try:

            predicates = tuple(get_where_predicates_from_query(query))
            if len(predicates):
                where_operator_usage[predicates] += 1
            
            parsed_query = Parser(query)
            columns = parsed_query.columns_dict

            # Ignore pg system queries (touches any table that starts with "pq_")
            if any(map(lambda x : x.startswith("pg_"), parsed_query.tables)):
                continue

            # If only one table, prefix is not there; append
            table_prefix = ""
            if len(parsed_query.tables) == 1:
                table_prefix = parsed_query.tables[0] + "."

            for operator_type in columns:
                for column in columns[operator_type]:
                    if "." in column:
                        column_usage[operator_type][column] += 1
                    else:
                        column_usage[operator_type][table_prefix + column] += 1

            # Update counters
            parsing_success["success"] += 1

        except Exception as e:
            parsing_success["failure"] += 1

    return parsing_success, column_usage, where_operator_usage
