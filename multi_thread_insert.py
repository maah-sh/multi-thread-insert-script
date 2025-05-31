import csv
import time
from threading import Thread, Lock
import sqlite3


lock = Lock()


def create_table_personal_info():
    with sqlite3.connect('db.sqlite3') as conn:
        create_statement = '''CREATE TABLE IF NOT EXISTS personal_info (
                            id INTEGER PRIMARY KEY, 
                            first_name text NOT NULL, 
                            last_name text NOT NULL, 
                            age INT NOT NULL
                        );'''
        
        cursor = conn.cursor()
        cursor.execute(create_statement)   
        conn.commit()


def insert_to_personal_info(row):
    with lock:
        with sqlite3.connect('db.sqlite3') as conn:
            insert_statement = ''' INSERT INTO personal_info(first_name,last_name,age)
                    VALUES(?,?,?) '''
            
            cursor = conn.cursor()
            cursor.executemany(insert_statement, row)
            # cursor.execute(insert_statement, row)
            conn.commit()


def insert_from_list(rows):
    for row in rows:
        insert_to_personal_info(row)


def enable_wall_mode():
    with sqlite3.connect('db.sqlite3') as conn:
        conn.execute('PRAGMA journal_mode=WAL;')


if __name__ == '__main__':
    
    start_time = time.time()

    with open('personal_info_entries.csv', 'r') as file:
        reader = csv.reader(file)
        entries_list = [tuple(row) for row in reader]
    
    n = len(entries_list)

    create_table_personal_info()
    
    # enable_wall_mode()

    # t1 = Thread(target=insert_from_list, args=(entries_list[0:n//2], ))
    # t2 = Thread(target=insert_from_list, args=(entries_list[n//2:n], ))

    t1 = Thread(target=insert_to_personal_info, args=(entries_list[0:n//2], ))
    t2 = Thread(target=insert_to_personal_info, args=(entries_list[n//2:n], ))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    end_time = time.time()

    print(end_time - start_time)
        




    


