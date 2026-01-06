import sqlite3
import logging
from datetime import datetime

#The logging setup
logging.basicConfig(
    filename='/Users/bella/factory_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("--- AUTOMATION FACTORY STARTING---")

#The database connection
connection = sqlite3.connect('cleanup_v2.db')
cursor = connection.cursor()

#Create authors table
try:
    cursor.execute ('''
                CREATE TABLE IF NOT EXISTS authors (
                author_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE)
''')

#Create books table with intergiry check using UNIQUE
    cursor.execute ('''
                CREATE TABLE IF NOT EXISTS books (
                book_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                author_id INTEGER,
                FOREIGN KEY (author_id) REFERENCES authors (author_id)
                UNIQUE(title, author_id)
)                
''')

# Handle Author First (Required to get the ID)
    cursor.execute("INSERT OR IGNORE INTO authors (name) VALUES ('J.R.R. Tolkien')")
    cursor.execute("SELECT author_id FROM authors WHERE name = 'J.R.R. Tolkien'")
    author_id = cursor.fetchone()[0]

# Add the Second Author
    cursor.execute("INSERT OR IGNORE INTO authors (name) VALUES ('C.S. Lewis')")
    cursor.execute("SELECT author_id FROM authors WHERE name = 'C.S. Lewis'")
    author_id_lewis = cursor.fetchone()[0]

# Now that we have author_id, define the Shipment
    book_list = [
        ('The Hobbit', author_id),
        ('The Fellowship of the Ring', author_id),
        ('The Two Towers', author_id),
        ('The Return of the King', author_id),
        ('A Leaf by Niggle', author_id)
    ]

# Define the Second Shipment
    lewis_books = [
    ('The Lion, the Witch and the Wardrobe', author_id_lewis),
    ('Prince Caspian', author_id_lewis),
    ('The Voyage of the Dawn Treader', author_id_lewis)
    ]

# Use executemany to process the whole list at once
    cursor.executemany('''
    INSERT OR IGNORE INTO books (title, author_id)
    VALUES (?, ?)
''', book_list)

# Batch Process the second shipment
    cursor.executemany('''
    INSERT OR IGNORE INTO books (title, author_id)
    VALUES (?, ?)
''', lewis_books)


#author_id = cursor.lastrowid

    connection.commit()
    logging.info(f"SUCCESS: {len(book_list) + len(lewis_books)} Books Processed.")

except Exception as e:
    logging.error(f"DATABASE ERROR: {e}")

# --- BLOCK 1: THE AUTOMATION TRY ---
try:
    # ... all your code for adding Tolkien and Lewis ...
    connection.commit()
    logging.info("AUTOMATION: All books processed.")
except Exception as e:
    logging.error(f"AUTOMATION ERROR: {e}")

# --- BLOCK 2: THE MAINTENANCE TRY ---
try:
    book_to_remove = 'A Leaf by Niggle'
    cursor.execute("DELETE FROM books WHERE title = ?", (book_to_remove,))
    
    if cursor.rowcount > 0:
        logging.info(f"MAINTENANCE: '{book_to_remove}' removed.")
    
    connection.commit()
except Exception as e:
    logging.error(f"MAINTENANCE ERROR: {e}")


#The REPORT (The JOIN)
cursor.execute('''
SELECT books.title, authors.name
FROM books
Join authors ON books.author_id = authors.author_id
''')

all_results = cursor.fetchall()

#Instead of print (f"BOOK: {row[0]} | AUTHOR: {row[1]}")
for row in all_results:
    logging.info(f"REPORT: Book Found: {row[0]} | Author: {row[1]}")
logging.info("--- FACTORY FINISHED---")
connection.close()