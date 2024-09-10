# REGULAR EXPRESSION BASED APPROACH

# Importing the regular expression library
import re

# Defining a new user-defined function
def extract_sql_queries(scala_script_path, sql_output_path):
    
    # Read file
    with open(scala_script_path, 'r') as file:
        scala_script = file.read()
        
    # Setting standard expression parameters aligning to the sql queries
    sql_pattern = re.compile(r'(\.table\s*\(\s*".*?"\s*\)|\.sql\s*\(\s*".*?"\s*\)|select\s*\(.+?from\s*.+?\))', re.DOTALL | re.IGNORECASE)
    matches = sql_pattern.findall(scala_script)
    sql_queries = []
    for match in matches:
        if '.table' in match or '.sql' in match:
            query = re.search(r'["\'](.+?)["\']', match)
            if query:
                sql_queries.append(query.group(1))
        else:
            sql_queries.append(match)
    sql_output = "\n\n".join(sql_queries)
    with open(sql_output_path, 'w') as file:
        file.write(sql_output)
        
# Specifying the input and output file names
scala_script_path = 'input.scala'
sql_output_path = 'query.sql'

extract_sql_queries(scala_script_path, sql_output_path)