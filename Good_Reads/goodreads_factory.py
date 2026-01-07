import sqlite3
import csv
import os
import logging

#Logging Configarution
logging.basicConfig(
    filename = 'goodreads_log.txt',
    level=logging.INFO, 
    format='%(asctime)s,%(levelname)s,%(message)s')
logging.info("--- GOODREADS ETL STARTING---")
print("Starting Goodreads ETL process...")
folder_path = 'DATA_CSV'
connection = sqlite3.connect('goodreads_v2.db')
cursor = connection.cursor()
print("✓ Database connected")
# Rename archive folder to DATA_CSV

logging.info(f" Checking folder: {folder_path}")
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
logging.info(f" Found {len(csv_files)} CSV files to process")
print(f"Found {len(csv_files)} CSV files")

# Function (Table Format)
def peak_at_data(file_path, num_rows=5):
    file_name = os.path.basename(file_path)
    logging.info(f"\n{'='*20} TABLE VIEW: {file_name} {'='*20}")
    
    # Header format: ID (8 chars), Name (40 chars), Rating (20 chars)
    row_fmt = "{:<8} | {:<40} | {:<20}"
    logging.info(row_fmt.format("ID", "NAME/TITLE", "RAW RATING"))
    logging.info("-" * 75)

    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= num_rows: break
            
            # Extract values safely
            id_val = row.get('Id') or row.get('ID') or "?"
            name = (row.get('Name')[:37] + '...') if len(row.get('Name', '')) > 37 else row.get('Name')
            rating = row.get('Rating', 'N/A')
            
            logging.info(row_fmt.format(id_val, name, rating))
    logging.info("="*75 + "\n")

# Run the Peek
book_sample = os.path.join(folder_path, 'book1-100k.csv')
rating_sample = os.path.join(folder_path, 'user_rating_0_to_1000.csv')
peak_at_data(book_sample)
peak_at_data(rating_sample)

# Create Tables (The Correct Way)
# In Books, the ID comes from the CSV, so no AUTOINCREMENT here.
cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        book_id TEXT PRIMARY KEY, 
        title TEXT,
        avg_rating REAL
    )
''')

# DELETE  OLD BROKEN TABLE FIRST
cursor.execute('DROP TABLE IF EXISTS ratings')

# In Ratings, we need a unique rating_id for every row.
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ratings (
        rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id TEXT,
        user_id TEXT,
        rating_text TEXT,
        rating_int INTEGER,
        FOREIGN KEY (book_id) REFERENCES books (book_id)
    )
''')

# The Translator Dictionary
RATING_MAP = {
    'it was amazing': 5,
    'really liked it': 4,
    'liked it': 3,
    'it was ok': 2,
    'did not like it': 1,
    'this user marked the book as "to-read"': 0
}

def load_ratings_to_db(file_path):
    logging.info(f"--- LOADING: {os.path.basename(file_path)} ---")
    
    with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        data_to_save = []
        
        for row in reader:
            # Clean the raw text from the CSV
            raw_text = row.get('Rating', '').lower().strip()
            
            # Use our Mapper to get the number
            numeric_rating = RATING_MAP.get(raw_text, 0)
            
            # Prepare the row for the database
            # (user_id, book_title, rating_text, rating_int)
            data_to_save.append((
                row.get('ID'), 
                row.get('Name'), 
                raw_text, 
                numeric_rating
            ))
            
        # Bulk Insert: Pushing all rows at once is 100x faster
        cursor.executemany('''
            INSERT INTO ratings (book_id, user_id, rating_text, rating_int)
            VALUES (?, ?, ?, ?)
        ''', data_to_save)
        
        connection.commit()
        logging.info(f"✓ Saved {len(data_to_save)} ratings to database")

# Let's test it on our sample
load_ratings_to_db(rating_sample)
connection.commit()

# CLOSE THE CONNECTION
connection.close()
print("Process Complete. Check goodreads_log.txt for details!")
logging.info("Database updated successfully.")