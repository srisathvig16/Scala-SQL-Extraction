import re

def extract_sql_queries(scala_script_path, sql_output_path):
    with open(scala_script_path, 'r') as file:
        lines = file.readlines()
    
    sql_queries = []
    collecting = False
    current_query = []

    # Keywords that signal the start of SQL-related code
    sql_start_indicators = ['select', 'from', 'where', 'join', 'group by', 'order by']
    
    for line in lines:
        stripped_line = line.strip()
        
        # Check if the line contains any SQL start indicators
        if any(indicator in stripped_line.lower() for indicator in sql_start_indicators):
            collecting = True
        
        if collecting:
            current_query.append(line.strip())
        
        # Check if the current line signifies the end of a SQL query
        if stripped_line.endswith(')') or stripped_line.endswith(';'):
            if collecting:
                # Process the collected lines into a single query
                query = " ".join(current_query)
                # Perform additional formatting if necessary
                formatted_query = format_sql_query(query)
                sql_queries.append(formatted_query)
                current_query = []
                collecting = False
    
    # Write the extracted SQL queries to the output file
    with open(sql_output_path, 'w') as file:
        for query in sql_queries:
            file.write(query + ";\n\n")

def format_sql_query(query):
    # Example formatting logic: you might need to adjust this based on your exact needs
    query = query.replace('$', '')
    query = query.replace('.table', '')
    query = query.replace('.withColumn', '')
    query = query.replace('.select', '')
    query = query.replace('"', '')
    # Further processing to handle specific DataFrame methods and convert them to SQL
    query = query.replace('==', '=')
    query = re.sub(r'\.\s*', '', query)
    query = re.sub(r'\s+', ' ', query).strip()
    
    # Convert method chaining to SQL-like format
    if 'val ' in query:
        query = query.split('=')[-1].strip()
    if '.join(' in query:
        query = handle_join(query)
    
    return query

def handle_join(query):
    # Example join handling logic
    # Convert DataFrame join syntax to SQL JOIN
    join_match = re.search(r'\.join\(([^,]+),\s*([^,]+)\s*,\s*(\w+)\)', query)
    if join_match:
        left_table = join_match.group(1).strip()
        right_table = join_match.group(2).strip()
        join_type = join_match.group(3).strip().upper()
        query = f"{left_table} {join_type} JOIN {right_table} ON "
        join_conditions = re.findall(r'(\w+\s*==\s*\w+)', query)
        query += " AND ".join(join_conditions)
    return query

# Example usage
scala_script_path = 'input.scala'
sql_output_path = 'query3.sql'

extract_sql_queries(scala_script_path, sql_output_path)