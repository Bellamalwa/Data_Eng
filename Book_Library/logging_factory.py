import sqlite3
import logging
from datetime import datetime

#The logging setup
logging.basicConfig(
    filename='/Users/bella/factory_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("--- FACTORY STARTING---")

#The database connection
connection = sqlite3.connect('logging_v2.db')
cursor = connection.cursor()

#Create authors table
try:
    cursor.execute ('''
                CREATE TABLE IF NOT EXISTS authors (
                author_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE)
''')

#Create books table 
    cursor.execute ('''
                CREATE TABLE IF NOT EXISTS books (
                book_id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                author_id INTEGER,
                FOREIGN KEY (author_id) REFERENCES authors (author_id)
)                
''')

#Add the Author
    cursor.execute("INSERT OR IGNORE INTO authors (name) VALUES ('J.R.R. Tolkien')")

#author_id = cursor.lastrowid

#Find that Authors ID number
    cursor.execute('''
               SELECT author_id FROM authors WHERE name = 'J.R.R. Tolkien'
''')
    author_id = cursor.fetchone()[0]

#Add book linked to that ID
    cursor.execute('''
               INSERT INTO books (title, author_id)
               values (?,?)
''' , ('The Hobbit', author_id))

    connection.commit()
    logging.info('''
             SUCCESS: Database updated succefully
''')

except Exception as e:
    logging.error(f"DATABASE ERROR: {e}")

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