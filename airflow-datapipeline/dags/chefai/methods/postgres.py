import psycopg2
import json
import os.path
import pymongo
from dotenv import load_dotenv
import os



def clear_tables(cur):
    # Clear the recipe_tags table
    cur.execute("""
        DELETE FROM recipe_tags
    """)

    # Clear the recipe_ingredients table
    cur.execute("""
        DELETE FROM recipe_ingredients
    """)


    # Clear the instructions table
    cur.execute("""
        DELETE FROM instructions
    """)

    # Clear the tags table
    cur.execute("""
        DELETE FROM tags
    """)

    # Clear the ingredients table
    cur.execute("""
        DELETE FROM ingredients
    """)

    # Clear the embeddings table
    cur.execute("""
        DELETE FROM embeddings
    """)

    # Clear the recipe table
    cur.execute("""
        DELETE FROM recipe
    """)

def drop_tables(cur):
    # Drop the recipe_tags table
    cur.execute("""
        DROP TABLE IF EXISTS recipe_tags
    """)

    # Drop the recipe_ingredients table
    cur.execute("""
        DROP TABLE IF EXISTS recipe_ingredients
    """)

    # Drop the emeddings table
    cur.execute("""
        DROP TABLE IF EXISTS embeddings
    """)


    # Drop the instructions table
    cur.execute("""
        DROP TABLE IF EXISTS instructions
    """)


    # Drop the tags table
    cur.execute("""
        DROP TABLE IF EXISTS tags
    """)

    # Drop the ingredients table
    cur.execute("""
        DROP TABLE IF EXISTS ingredients
    """)

    # Drop the recipe table
    cur.execute("""
        DROP TABLE IF EXISTS recipe
    """)



def create_tables(cur):
     # Create the recipe table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            url_name VARCHAR(255),
            url VARCHAR(255),
            status VARCHAR(255),
            description TEXT,
            image_url VARCHAR(255),
            created_at DATE NOT NULL DEFAULT CURRENT_DATE
        )
    """)

    # Create the instructions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS instructions (
            position INT,
            content TEXT,
            recipe_id INT,
            PRIMARY KEY (position, recipe_id),
            FOREIGN KEY (recipe_id) REFERENCES recipe(id)
        )
    """)

    # Create the tags table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            name VARCHAR(255) PRIMARY KEY
        )
    """)

    # Create the recipe_tags table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_tags (
            recipe_id INT,
            tag_name VARCHAR(255),
            PRIMARY KEY (recipe_id, tag_name),
            FOREIGN KEY (recipe_id) REFERENCES recipe(id),
            FOREIGN KEY (tag_name) REFERENCES tags(name)
        )
    """)

    # Create the ingredients table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ingredients (
            name VARCHAR(255) PRIMARY KEY
        )
    """)

    # Create the recipe_ingredients table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            recipe_id INT,
            ingredient_name VARCHAR(255),
            units VARCHAR(255),
            amount VARCHAR(255),
            notes VARCHAR(255),
            PRIMARY KEY (recipe_id, ingredient_name),
            FOREIGN KEY (recipe_id) REFERENCES recipe(id),
            FOREIGN KEY (ingredient_name) REFERENCES ingredients(name)
        )
    """)

    # Create the embeddings table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS embeddings (
            recipe_id INT,
            type VARCHAR(255),
            embedding JSON,
            PRIMARY KEY (recipe_id, type),
            FOREIGN KEY (recipe_id) REFERENCES recipe(id)
        )
    """)

def insert_recipe(cur, recipe):
    # Insert the recipe
    cur.execute("""
        INSERT INTO recipe (name, url_name, url, status, description, image_url)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """, (
        recipe['new_name'],
        recipe['url_name'],
        recipe['url'],
        recipe['status'],
        recipe['description'],
        recipe['image_url']
    ))
    recipe_id = cur.fetchone()[0]

    # Insert the instructions
    cur.executemany("""
        INSERT INTO instructions (position, content, recipe_id)
        VALUES (%s, %s, %s)
    """, [
        (position, content, recipe_id)
        for position, content in enumerate(recipe['instructions_chat'])
    ])

    # Insert the tags
    cur.executemany("""
        INSERT INTO tags (name)
        VALUES (%s)
        ON CONFLICT DO NOTHING
    """, [
        (tag,)
        for tag in recipe['tags']
    ])

    # Insert the recipe_tags
    cur.executemany("""
        INSERT INTO recipe_tags (recipe_id, tag_name)
        VALUES (%s, %s)
    """, [
        (recipe_id, tag)
        for tag in recipe['tags']
    ])

    # Insert the ingredients
    cur.executemany("""
        INSERT INTO ingredients (name)
        VALUES (%s)
        ON CONFLICT DO NOTHING
    """, [
        (ingredient['name'],)
        for ingredient in recipe['ingredients_list']
    ])

    # Insert the recipe_ingredients
    cur.executemany("""
        INSERT INTO recipe_ingredients (recipe_id, ingredient_name, units, amount, notes)
        VALUES (%s, %s, %s, %s, %s)
    """, [
        (recipe_id, ingredient['name'], ingredient['unit'], ingredient['amount'], ingredient['notes'])
        for ingredient in recipe['ingredients_list']
    ])

    # Insert the embeddings
    embedding_type = 'glove'
    cur.execute("""
        INSERT INTO embeddings (recipe_id, type, embedding)
        VALUES (%s, %s, %s)
    """, (
        recipe_id,
        embedding_type,
        json.dumps(recipe['embedding'])
    ))


    


if __name__ == "__main__":
    # Local connection for testing purposes.
    load_dotenv()

    conn = psycopg2.connect(
        host="localhost",
        database="chefai",
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cur = conn.cursor()
    # create_tables(cur)
    clear_tables(cur)
    drop_tables(cur)
    create_tables(cur)

    client = pymongo.MongoClient(os.getenv("MONGO_URI_CLOUD"))
    db = client["recipes"]
    collection = db["recipe_production"]
    recipes = collection.find({})
    for recipe_info in recipes:
        insert_recipe(cur, recipe_info)


    cur.execute("""SELECT * FROM recipe where id < 3""")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.commit()
    cur.close()
    conn.close()