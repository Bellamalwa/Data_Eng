import sqlite3

# Creating a file named 'my_library.db'
connection = sqlite3.connect('my_library.db')

# We use cursor to pick and move blocks
cursor = connection.cursor()

# Create a table to execute a command 
cursor.execute('''
               CREATE TABLE IF NOT EXISTS books (
               book_id INTEGER PRIMARY KEY AUTOINCREMENT,
               title TEXT UNIQUE,
               year INTEGER,
               checked_out INTEGER
            )
''')
books_to_add = [
    ('The Hobbit', 1937, 0),
    ('1984', 1949, 0)
]

#Adding the books use INSERT OR IGNORE 
#This means id theres duplicate, just skip it 
cursor.executemany('''
               INSERT OR IGNORE INTO books (title, year, checked_out)
               VALUES (?,?,?)
''', books_to_add)

#Use commit like a save button
connection.commit()

#Ask the question
cursor.execute("SELECT book_id, title, year FROM books")

#The cursor grabs all the results 
all_books = cursor.fetchall()

#print out to the screen
for book in all_books:
    print(F"ID: {book[0]} | Title: {book[1]} | Year: {book[2]}")

#Close at the end
connection.close()    