import sqlite3
import csv
import os
import logging
import argparse
import sys

# 1. CONFIGURABILITY (Argument Parsing)
parser = argparse.ArgumentParser(description="Professional Goodreads ETL Pipeline")
parser.add_argument('--folder', default='DATA_CSV', help='Source folder for CSVs')
parser.add_argument('--db', default='goodreads_v2.db', help='Target SQLite Database name')
args = parser.parse_args()

# 2. LOGGING CONFIGURATION (Fresh start every run)
logging.basicConfig(
    filename = 'goodreads_log.txt',
    filemode = 'w', 
    level = logging.INFO, 
    format = '%(asctime)s,%(levelname)s,%(message)s')

logging.info("--- GOODREADS ETL STARTING ---")
print("Starting Goodreads ETL process...")

# 3. TESTING (Unit Test for Rating Map - Engineering Points)
RATING_MAP = {
    'it was amazing': 5, 
    'really liked it': 4, 
    'liked it': 3,
    'it was ok': 2, 
    'did not like it': 1, 
    'this user marked the book as "to-read"': 0
}

def run_unit_tests():
    try:
        assert RATING_MAP['liked it'] == 3
        assert RATING_MAP['it was amazing'] == 5
        logging.info("✅ Pre-run unit tests passed.")
    except AssertionError:
        logging.critical("❌ Logic Test Failed! RATING_MAP is incorrect.")
        sys.exit(1)

# 4. VISUAL PEAK FUNCTION (Explicitly for both Books and Ratings)
def peak_at_data(file_path, num_rows=5):
    file_name = os.path.basename(file_path)
    logging.info(f"\n{'='*20} TABLE VIEW: {file_name} {'='*20}")
    row_fmt = "{:<8} | {:<40} | {:<20}"
    logging.info(row_fmt.format("ID", "NAME/TITLE", "RAW RATING"))
    logging.info("-" * 75)

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= num_rows: break
                id_val = row.get('Id') or row.get('ID') or "?"
                name = (row.get('Name')[:37] + '...') if len(row.get('Name', '')) > 37 else row.get('Name')
                rating = row.get('Rating', 'N/A')
                logging.info(row_fmt.format(id_val, name, rating))
        logging.info("="*75 + "\n")
    except Exception as e:
        logging.error(f"Error peaking at {file_name}: {e}")

# 5. MAIN ETL ENGINE
def run_etl():
    run_unit_tests()
    connection = sqlite3.connect(args.db)
    cursor = connection.cursor()

    # Schema Setup
    cursor.execute('CREATE TABLE IF NOT EXISTS books (book_id TEXT PRIMARY KEY, title TEXT, avg_rating REAL)')
    cursor.execute('DROP TABLE IF EXISTS ratings')
    cursor.execute('''
        CREATE TABLE ratings (
            rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id TEXT, user_id TEXT, rating_text TEXT, rating_int INTEGER,
            FOREIGN KEY (book_id) REFERENCES books (book_id))
    ''')

    # Ensure your specific files appear first in the log
    target_book = os.path.join(args.folder, 'book1-100k.csv')
    target_rating = os.path.join(args.folder, 'user_rating_0_to_1000.csv')
    
    if os.path.exists(target_book): peak_at_data(target_book)
    if os.path.exists(target_rating): peak_at_data(target_rating)

    csv_files = sorted([f for f in os.listdir(args.folder) if f.endswith('.csv')])
    print(f"Found {len(csv_files)} files. Processing all data...")

    for filename in csv_files:
        path = os.path.join(args.folder, filename)
        try:
            with open(path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                
                # VALIDATION: Check for required columns in both types
                if filename.startswith('book'):
                    if 'Id' not in reader.fieldnames:
                        logging.warning(f"⚠️ Skipping {filename}: Missing 'Id' column")
                        continue
                    
                    data = [(row.get('Id'), row.get('Name'), row.get('Rating')) for row in reader if row.get('Id')]
                    cursor.executemany('INSERT OR IGNORE INTO books VALUES (?,?,?)', data)
                    logging.info(f"✓ Processed {len(data)} books from {filename}")
                
                elif filename.startswith('user_rating'):
                    if 'ID' not in reader.fieldnames:
                        logging.warning(f"⚠️ Skipping {filename}: Missing 'ID' column")
                        continue
                        
                    data = []
                    for row in reader:
                        txt = row.get('Rating', '').lower().strip()
                        data.append((row.get('Name'), row.get('ID'), txt, RATING_MAP.get(txt, 0)))
                    cursor.executemany('INSERT INTO ratings (book_id, user_id, rating_text, rating_int) VALUES (?,?,?,?)', data)
                    logging.info(f"✓ Processed {len(data)} ratings from {filename}")

            connection.commit()
        except Exception as e:
            logging.error(f"❌ Error in file {filename}: {e}")

    # 6. ADVANCED INSIGHTS (SQL Joins)
    print("\n" + "═"*50 + "\n📊 GOODREADS ETL INSIGHTS\n" + "═"*50)
    
    cursor.execute("SELECT COUNT(*) FROM books")
    b_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ratings")
    r_total = cursor.fetchone()[0]
    print(f"Database Size: {b_total:,} Books and {r_total:,} User Ratings.")

    print("\n🔥 MOST REVIEWED BOOKS (JOINED DATA):")
    # This query joins the two tables to find actual user engagement
    query = '''
        SELECT b.title, COUNT(r.rating_id) as reviews 
        FROM books b JOIN ratings r ON b.title = r.book_id 
        GROUP BY b.title ORDER BY reviews DESC LIMIT 5
    '''
    cursor.execute(query)
    for title, count in cursor.fetchall():
        print(f"• {title[:40]:<40} | {count:,} reviews")

    print("═"*50)
    connection.close()
    logging.info('=='*50)
    logging.info("ALL FILES PROCESSED")
    logging.info('=='*50)
    print("Process Complete. Check goodreads_log.txt for detail tables.")

if __name__ == "__main__":
    run_etl()