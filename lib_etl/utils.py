

def get_query_from_file(file_sql):

    with open(file_sql) as f:
        query = f.readlines()
        query = "".join(query)

    return query