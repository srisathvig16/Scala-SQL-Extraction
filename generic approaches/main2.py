# FUNCTION BASED APPROACH

# Defining a new user-defined function
def extract_sql_queries(scala_script_path, sql_output_path):
    
    # Read file
    with open(scala_script_path, 'r') as file:
        lines = file.readlines()
    
    sql_queries = []
    collecting = False
    current_query = []
    
    # Specify sql functions as per the input file
    sql_start_indicators = ['select', 'from', 'where', 'join', 'group by', 'order by']
    
    # Checks if the line contains any SQL functions
    for line in lines:
        stripped_line = line.strip().lower()
        if any(indicator in stripped_line for indicator in sql_start_indicators):
            collecting = True
        
        if collecting:
            current_query.append(line)
        
        if stripped_line.endswith(')') or stripped_line.endswith(';'):
            if collecting:
                sql_queries.append("".join(current_query))
                current_query = []
                collecting = False
    
    # Joins all extracted SQL queries into a single string
    sql_output = "\n\n".join(sql_queries)
    with open(sql_output_path, 'w') as file:
        file.write(sql_output)

# Specifying the input and output file names
scala_script_path = 'input.scala'
sql_output_path = 'query2.sql'

extract_sql_queries(scala_script_path, sql_output_path)