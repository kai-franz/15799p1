
def drop_indexes(threshold):
    """
    Drop indexes that are not used often in the current workload.
    :param threshold: The threshold for the number of times an index is
    scanned below which it is dropped.
    :return: SQL statement to drop indexes.
    Based on https://www.cybertec-postgresql.com/en/get-rid-of-your-unused-indexes/
    """

    return ("DO $$ DECLARE index RECORD; BEGIN FOR index IN " +
            "(SELECT s.indexrelname AS name " + "FROM pg_catalog.pg_stat_user_indexes s " +
            "JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid " +
            f"WHERE s.idx_scan <= {threshold} " +
            "AND NOT i.indisunique " +
            "AND NOT EXISTS " +
            "(SELECT 1 FROM pg_catalog.pg_constraint c " +
            "WHERE c.conindid = s.indexrelid)) " +
            "LOOP EXECUTE 'DROP INDEX ' || index.name; END LOOP; END; $$;\n")


def create_index(table_name, column_name):
    """
    Create an index on a column, if such an index does not already exist.
    Note that this function creates a single-column index, and the index
    will not be created if the target column is part of an existing
    multi-column index.

    :param table_name: Name of table to create index on.
    :param column_name: Name of column to create index on.
    :return: SQL statement to create index.
    """
    return ("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_class, pg_index, pg_attribute WHERE pg_class.oid = pg_index.indrelid AND "
            "pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = ANY(pg_index.indkey) AND pg_class.relkind = 'r' "
            "AND pg_class.relname = '{0}' AND pg_attribute.attname = '{1}') THEN ".format(table_name, column_name) +
            "CREATE INDEX {0}_{1}_idx ON {0} USING btree ({1}); END IF; END; $$;\n".format(table_name, column_name))

