import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Embedding, TimeDistributed
import numpy as np
import re

# Load and preprocess data
def load_data(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    return data

def extract_sql_statements(scala_code):
    # Extract SQL-like statements
    sql_statements = re.findall(r'\.(select|withColumn|join|filter|where|groupBy|orderBy|agg)\(.*?\)', scala_code, re.DOTALL)
    return sql_statements

def prepare_dataset(scala_code, sql_queries):
    if not scala_code or not sql_queries:
        raise ValueError("No Scala code or SQL queries found.")
    
    # Ensure equal length sequences
    X = [list(code) for code in scala_code]
    y = [list(query) for query in sql_queries]
    
    max_seq_length = max(max(len(seq) for seq in X), max(len(seq) for seq in y))
    vocab = sorted(set(''.join([''.join(seq) for seq in X]) + ''.join([''.join(seq) for seq in y])))
    
    char_to_idx = {char: idx for idx, char in enumerate(vocab)}
    idx_to_char = {idx: char for char, idx in char_to_idx.items()}
    
    X = [[char_to_idx[char] for char in seq] + [0] * (max_seq_length - len(seq)) for seq in X]
    y = [[char_to_idx[char] for char in seq] + [0] * (max_seq_length - len(seq)) for seq in y]
    
    return np.array(X), np.array(y), char_to_idx, idx_to_char, max_seq_length

def build_model(vocab_size, seq_length):
    model = Sequential([
        Embedding(input_dim=vocab_size, output_dim=256, input_length=seq_length),
        LSTM(128, return_sequences=True),
        LSTM(128, return_sequences=True),
        TimeDistributed(Dense(vocab_size, activation='softmax'))
    ])
    model.compile(optimizer='adam', loss='sparse_categorical_crossentropy')
    return model

def train_model(model, X, y, epochs=10, batch_size=32):
    y = np.expand_dims(y, -1)  # Ensure y has correct shape
    model.fit(X, y, epochs=epochs, batch_size=batch_size)

def generate_sql(model, scala_code, char_to_idx, idx_to_char, max_seq_length):
    X = [char_to_idx.get(char, 0) for char in scala_code] + [0] * (max_seq_length - len(scala_code))
    X = np.array(X).reshape((1, max_seq_length))
    
    y_pred = model.predict(X)
    sql_query = ''.join([idx_to_char[np.argmax(char_prob)] for char_prob in y_pred[0]])
    
    return sql_query

# Main script
scala_file_path = 'input.scala'  # Updated to .txt file
output_sql_file_path = 'output_queries.sql'

scala_code = load_data(scala_file_path)
sql_statements = extract_sql_statements(scala_code)

if not sql_statements:
    raise ValueError("No SQL statements found in the provided Scala code.")

X, y, char_to_idx, idx_to_char, max_seq_length = prepare_dataset(sql_statements, sql_statements)

vocab_size = len(char_to_idx)
model = build_model(vocab_size, max_seq_length)
train_model(model, X, y)

# Inference
scala_code = load_data(scala_file_path)
sql_statements = extract_sql_statements(scala_code)

with open(output_sql_file_path, 'w') as sql_file:
    for scala_code in sql_statements:
        sql_query = generate_sql(model, scala_code, char_to_idx, idx_to_char, max_seq_length)
        sql_file.write(sql_query + ';\n')

print(f'SQL queries have been saved to {output_sql_file_path}')