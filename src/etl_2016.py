import csv
import sqlite3

def extract():
    data = []
    with open('data/input.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

def transform(data):
    transformed = []
    for row in data:
        # Simple transformation: capitalize names
        row['name'] = row['name'].upper()
        transformed.append(row)
    return transformed

def load(data):
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users
        (name TEXT, age INTEGER)
    ''')
    
    # Insert data
    for row in data:
        cursor.execute('INSERT INTO users VALUES (?, ?)',
                      (row['name'], int(row['age'])))
    
    conn.commit()
    conn.close()

if __name__ == '__main__':
    # Run ETL process
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
    print("ETL process completed!")
