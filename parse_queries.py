from pglast import parse_sql
from pglast import Node


def parse_cols(query_string, cols):
    """
    Parse a query string and track the number of times each column is referenced.
    :param query_string: SQL query string to parse
    :param cols: dictionary of (table, column) -> count
    :return: None
    """
    ast = parse_sql(query_string)
    # construct alias map
    alias_map = {}
    for node in Node(ast).traverse():
        if repr(node).find('Alias') != -1:
            alias_map[node.aliasname.value] = node.parent_node.relname.value

    # traverse AST, checking for column references
    for node in Node(ast).traverse():
        if repr(node).find('{ColumnRef}') != -1 and repr(node.fields).find('{A_Star}') == -1:
            fields = []
            for field in node.fields:
                if len(fields) == 0 and field.val.value in alias_map:
                    fields.append(alias_map[field.val.value])
                else:
                    fields.append(field.val.value)
            if len(fields) == 2:
                cols[tuple(fields)] += 1
