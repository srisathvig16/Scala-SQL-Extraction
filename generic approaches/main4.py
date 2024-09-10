import re

def extract_sql_statements(scala_code):
    sql_statements = []
    in_string = False
    in_comment = False
    current_sql = ''
    prev_char = ''

    for char in scala_code:
        if char == '"' or char == "'":
            if prev_char != '\\':
                in_string = not in_string
        elif char == '/' and prev_char == '/':
            in_comment = True
        elif char == '\n' and in_comment:
            in_comment = False
        elif char == '*' and prev_char == '/':
            in_comment = False

        if not in_string and not in_comment:
            if char == ';':
                if current_sql:
                    sql_statements.append(current_sql.strip())
                    current_sql = ''
            elif char.isalpha() or char in [' ', '(', ')', ',', '.', '=']:
                current_sql += char

        prev_char = char

    if current_sql:
        sql_statements.append(current_sql.strip())

    return sql_statements

def write_sql_file(sql_statements, output_file):
    with open(output_file, 'w') as file:
        for statement in sql_statements:
            file.write(statement + ';\n')

def main():
    input_file = 'input.scala'
    output_file = 'query4.sql'

    with open(input_file, 'r') as file:
        scala_code = file.read()

    sql_statements = extract_sql_statements(scala_code)
    write_sql_file(sql_statements, output_file)

if __name__ == '__main__':
    main()