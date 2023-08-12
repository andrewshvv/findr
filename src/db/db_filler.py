import random
import sqlite3

# Database configuration
DB_NAME = 'db.sqlite'


def create_connection():
    conn = sqlite3.connect(DB_NAME)
    return conn


def insert_records():
    conn = create_connection()
    cursor = conn.cursor()

    user_id = 5881529475
    num_inserts = 10

    for _ in range(num_inserts):
        prompt_id = random.randint(1, 50)  # Adjust range if necessary
        post_id = random.randint(1, 15)
        cursor.execute(
            f"INSERT INTO users_posts (user_id, prompt_id, post_id, post_status, process_status) VALUES(?, ?, ?, 'new', 'accepted');",
            (user_id, prompt_id, post_id))

    conn.commit()
    conn.close()


def update_post_status(user_id, post_id, new_status):
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute("UPDATE users_posts SET post_status = ? WHERE user_id = ? AND post_id = ?;",
                   (new_status, user_id, post_id))

    conn.commit()
    conn.close()


def update_process_status(user_id, post_id, new_status):
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute("UPDATE users_posts SET process_status = ? WHERE user_id = ? AND post_id = ?;",
                   (new_status, user_id, post_id))

    conn.commit()
    conn.close()


def insert_records(n):
    conn = sqlite3.connect('db.sqlite')
    cursor = conn.cursor()

    prompt_id = 2
    post_status = 'new'
    process_status = 'accepted'
    user_id = 5881529475
    gpt_reason = 'Это причина от gpt'
    cursor.execute('''
        DELETE FROM users_posts
        ''')
    for post_id in range(1, n + 1):
        cursor.execute('''
        INSERT INTO users_posts (user_id, prompt_id, post_id, post_status, process_status, gpt_reason)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_id, prompt_id, post_id, post_status, process_status, gpt_reason))

    conn.commit()
    conn.close()

# if __name__ == "__main__":
#     # Insert records
#     # insert_records()
#
#     # Change post_status for user_id 5881529475 and post_id 5 to 'updated'
#     # update_post_status(5881529475, 5, 'updated')
#
#     # Change process_status for user_id 5881529475 and post_id 5 to 'processed'
#     # update_process_status(5881529475, 5, 'processed')
#
#     # Fill user posts
# n = 10
# insert_records(n)
