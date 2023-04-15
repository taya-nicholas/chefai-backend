import json
import numpy as np
import random
import psycopg2
from numpy.linalg import norm
import os

conn = psycopg2.connect(
    host=os.environ["host_name"],
    database="postgres",
    user=os.environ["user_name"],
    password=os.environ["password"]
)
cur = conn.cursor()

cur.execute(""" select recipe_id from embeddings order by recipe_id""")
id_rows = cur.fetchall()
ids = [row[0] for row in id_rows]

def get_mealplan_score(inds):
    cur.execute("""select embedding from embeddings where recipe_id in %s""", (tuple(inds),))
    embedding_rows = cur.fetchall()
    embeddings = [row[0] for row in embedding_rows]

    sum_embeddings = []
    for i in range(len(embeddings)):
        for j in range(i+1, len(embeddings)):
            sum_embeddings.append(np.dot(embeddings[i], embeddings[j]) / (norm(embeddings[i]) * norm(embeddings[j])))
    meal_score = np.average(sum_embeddings)
    return meal_score


def lambda_handler(event, context):
    num_recipes = event["num_recipes"]
    iterations = event["iterations"]
    
    min_score = 1.0
    best_inds = None
    
    for i in range(iterations):
        inds = random.sample(ids, num_recipes)
        score = get_mealplan_score(inds)
        if score < min_score:
            min_score = score
            best_inds = inds
    
    return {
        'statusCode': 200,
        'body': best_inds
    }
