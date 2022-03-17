import pandas as pd
from collections import defaultdict
from parse_queries import parse_cols
from index_selection import create_index, drop_indexes


# percentage of the most used columns to be chosen as indexes
INDEX_CREATE_THRESHOLD = 10
# existing indexes that have been scanned this number of fewer times
# will be dropped
INDEX_DROP_THRESHOLD = 10


def task_project1():
    """
    Install modules
    """
    def task_project1_setup():

        return {

            "actions": [
                'sudo apt install pip3',
                'pip install pandas',
                'pip install pglast',
            ],

            "verbosity": 2,

        }

    """
    Generate actions.
    """
    def generate_indexes(workload_csv, timeout):
        print(f"dodo received workload CSV: {workload_csv}")
        print(f"dodo received timeout: {timeout}")

        # extract SQL queries from workload CSV
        log_df = pd.read_csv(workload_csv, header=None, usecols=[13], names=["query"])

        # strip out preceding 'statement: '
        sql_queries = log_df["query"].str[11:].astype(str)

        # remove anything that's not a SELECT, INSERT, or UPDATE
        sql_queries = list(filter(lambda x: x.upper().startswith(("SELECT", "INSERT", "UPDATE", "DELETE")), sql_queries.tolist()))

        # parse SQL queries, extracting all columns
        column_counts = defaultdict(int)

        for query in sql_queries:
            parse_cols(query, column_counts)

        sorted_columns = sorted(column_counts.items(), key=lambda x: x[1], reverse=True)

        with open("actions.sql", "w") as f:
            f.write(drop_indexes(INDEX_DROP_THRESHOLD))
            for i in range(len(sorted_columns) // INDEX_CREATE_THRESHOLD + 1):
                f.write(create_index(sorted_columns[i][0][0], sorted_columns[i][0][1]))


    return {
        "actions": [
            'echo "Tuning indexes..."',
            generate_indexes,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        "uptodate": [False],
        "verbosity": 2,
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "The PostgreSQL workload to optimize for.",
                "default": None,
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "The time allowed for execution before this dodo task will be killed.",
                "default": None,
            },
        ],
    }
