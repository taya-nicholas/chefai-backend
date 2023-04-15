# Import basic libraries
import requests
import logging
import openai
import time
import json
import numpy as np
import boto3
import psycopg2
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from typing import List, Set, Dict

# Import airflow libraries
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

# Import custom methods and constants
from chefai.methods.web_scraping import RecipeScraper
from chefai.methods.diffusion import RequestData
from chefai.methods.prompt import TAGS_TEMPLATE, INSTRUCTIONS_TEMPLATE
from chefai.methods import postgres



@dag(schedule="@weekly", start_date=datetime(2023, 3, 24), catchup=False)
def recipe_etl():

    # Define connection variables for mongo atlas
    db_name = "recipes"
    collection_name = "recipe_prod"
    meta_name = "meta_prod"
    conn_id = "mongo_atlas"

    # S3 bucket
    bucket_name = "chefai"

    # Get API keys and variables from airflow variables
    openai_apikey = Variable.get("openai_apikey")
    access_key = Variable.get("s3_access_key")
    secret_key = Variable.get("s3_secret")
    stable_api = Variable.get("stable_api_key")
    rds_user = Variable.get("rds_user")
    rds_password = Variable.get("rds_password")
    rds_host = Variable.get("rds_host")

    openai.api_key = openai_apikey

    
    def update_mongo_val(recipe_id: str, val_name: str, val: str) -> None:
        """Update a value in a mongo document

        Args:
            recipe_id (str): The id of the recipe to update
            val_name (str): The name of the value to update
            val (str): The new value
        """
        hook = MongoHook(conn_id)
        collection = hook.get_collection(collection_name, mongo_db=db_name)
        collection.update_one({'_id': ObjectId(recipe_id)}, {
                              '$set': {val_name: val}})

    def parse_instructions(instruction_string) -> List[str]:
        # Split the string by newline characters to get each line of instruction
        instruction_lines = instruction_string.split('\n')
        
        # Remove the 'Instructions:' line and any blank lines
        instruction_lines = [line.strip() for line in instruction_lines if line.strip() and line.strip() != 'Instructions:']
        
        # Remove the numbering from each line
        instruction_lines = [line.split('. ', 1)[1] if line[0].isdigit() else line for line in instruction_lines]
        
        return instruction_lines
    
    def parse_extra(extra_string):
        ind1 = extra_string.index('Tags:')
        ind2 = extra_string.index('Description:')
        ind3 = extra_string.index('Prompt:')
        ind4 = extra_string.index('Name:')

        tags_ori = extra_string[ind1:ind2].split('\n')[1].split(', ')
        tags = [tag.strip().lower() for tag in tags_ori]

        desc = extra_string[ind2:ind3].split('\n')[1].strip()

        prompt = extra_string[ind3:ind4].split('\n')[1].strip()

        name = extra_string[ind4:].split('\n')[1].strip()

        return tags, desc, prompt, name
    
    @task
    def setup():
        logging.info("Start: setup_task")
        # Create tables in SQL for later insertion
        try:
            conn = psycopg2.connect(
                        host=rds_host,
                        database="postgres",
                        user=rds_user,
                        password=rds_password
                    )

            cur = conn.cursor()
            postgres.create_tables(cur)
            
            conn.commit()
            cur.close()
            conn.close()
            logging.info("End: setup_task. Tables created successfully in PostgreSQL ")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise error    

    @task
    def scrape_recipes():
        logging.info("Start: scrape_recipes")

        hook = MongoHook(conn_id)
        collection = hook.get_collection(collection_name, mongo_db=db_name)
        meta = hook.get_collection(meta_name, mongo_db=db_name)
        scraper = RecipeScraper(collection, meta)
        scraper.run(page_limit=3)

        logging.info("End: scrape_recipes")


    @task
    def find_recipes() -> List[str]:
        logging.info("Start: find_recipes")

        hook = MongoHook(conn_id)
        collection = hook.get_collection(collection_name, mongo_db=db_name)

        # Find all recipes that have not been uploaded to SQL
        documents = collection.find({'status': {'$ne': '6_uploaded_to_SQL'}})
        id_list = [str(doc['_id']) for doc in documents]

        logging.info("End: find_recipes")
        return id_list

    @task
    def extract_from_mongo(recipe_id: str):
        logging.info(f"Start: extract_from_mongo. Extracting recipe {recipe_id}")

        hook = MongoHook(conn_id)
        coll = hook.get_collection(collection_name, mongo_db=db_name)
        recipe = coll.find_one({"_id": ObjectId(recipe_id)})
        recipe['_id'] = str(recipe['_id'])

        logging.info(f"End: extract_from_mongo. Extracted recipe: {recipe['name']}")
        return recipe
    
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def transform_chat(recipe_info: Dict[str, str]):

        if recipe_info['status'] == 'unprocessed':
            logging.info(f"Start: transform_chat. Transforming recipe {recipe_info['name']} with status {recipe_info['status']}")

            # Step 1: Improve the instructions
            try:
                instructions_message = f"{recipe_info}\n{INSTRUCTIONS_TEMPLATE}"
                completion = openai.ChatCompletion.create(
                    model = "gpt-3.5-turbo",
                    messages = [{"role": "system", "content": "You are a helpful assistant that completes your tasks to the best of your ability."}, {"role": "user", "content": instructions_message}],
                    temperature = 0.4,
                )
                response = completion.choices[0].message.content

                instructions_line = parse_instructions(response)

                logging.info(f"Chat task 1: Response: {response}")
                logging.info(f"Instructions: {instructions_line}")
            except Exception as e:
                logging.error(f"Error: {e}")
                raise
            else:
                # Step 2: Generate the tags, description, prompt, and name
                try:
                    extra_message = f"{response}\n{TAGS_TEMPLATE}"
                    completion = openai.ChatCompletion.create(
                        model = "gpt-3.5-turbo",
                        messages = [{"role": "system", "content": "You are a helpful assistant that completes your tasks to the best of your ability."}, {"role": "user", "content": extra_message}],
                        temperature = 0.6,
                    )
                    response_extra = completion.choices[0].message.content
                    tags, description, prompt, name = parse_extra(response_extra)

                    logging.info(f"Response extra: {response_extra}")
                    logging.info(f"Tags: {tags}")
                    logging.info(f"Description: {description}")
                    logging.info(f"Prompt: {prompt}")
                    logging.info(f"Name: {name}")
                except Exception as e:
                    logging.error(f"Error: {e}")
                    raise
                else:
                    new_recipe = recipe_info
                    new_recipe["instructions"] = instructions_line
                    new_recipe["tags"] = tags
                    new_recipe["description"] = description
                    new_recipe["prompt"] = prompt
                    new_recipe["new_name"] = name
                    new_recipe["status"] = "1_chat_transformed"

                    update_mongo_val(recipe_info['_id'], 'instructions_chat', instructions_line)
                    update_mongo_val(recipe_info['_id'], 'tags', tags)
                    update_mongo_val(recipe_info['_id'], 'description', description)
                    update_mongo_val(recipe_info['_id'], 'prompt', prompt)
                    update_mongo_val(recipe_info['_id'], 'new_name', name)
                    update_mongo_val(recipe_info['_id'], 'status', '1_chat_transformed')

                    logging.info(f"End transform_chat. Recipe '{recipe_info['name']}' has been successfully transformed.")
        else:
            new_recipe = recipe_info
            logging.info(f"End transform_chat. Recipe '{recipe_info['name']}' not transformed because status is {recipe_info['status']}")

        return new_recipe

    @task
    def transform_ingredients(recipe_info: Dict[str, str]):
        if recipe_info['status'] == "1_chat_transformed":
            logging.info(f"Start: transform_ingredients. Transforming recipe {recipe_info['name']} with status {recipe_info['status']}")
            try:
                ingredients = []
                for ingredient_group in recipe_info['ingredients']:
                    for ingredient in ingredient_group['ingredients']:
                        existing_ingredient = next(
                            (x for x in ingredients if x['name'] == ingredient['name']), None)
                        if existing_ingredient:
                            existing_ingredient['amount'] += ' ' + ingredient['amount']
                            if ingredient['notes']:
                                existing_ingredient['notes'] += ', ' + \
                                    ingredient['notes']
                        else:
                            ingredients.append({
                                'amount': ingredient['amount'],
                                'unit': ingredient['unit'],
                                'name': ingredient['name'],
                                'notes': ingredient['notes']
                            })
            except Exception as e:
                logging.error(f"Error: {e}")
                raise
            else:
                new_recipe = recipe_info
                new_recipe['status'] = '2_ingre_transformed'
                new_recipe['ingredients'] = ingredients
                logging.info(f"Transformed ingredients: {new_recipe['ingredients']}")

                update_mongo_val(recipe_info['_id'], 'status', '2_ingre_transformed')
                update_mongo_val(recipe_info['_id'], 'ingredients_list', ingredients)

                logging.info(f"End: transform_ingredients. Recipe '{recipe_info['name']}' has been successfully transformed.")
            
        else:
            new_recipe = recipe_info
            logging.info(f"End: transform_ingredients. Recipe '{recipe_info['name']}' has been already transformed with status {recipe_info['status']}.")

        return new_recipe
    

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def generate_recipe_image(recipe_info: Dict[str, str]):
        if recipe_info['status'] == "2_ingre_transformed":
            logging.info(f"Start: generate_recipe_image. Generating image for recipe {recipe_info['name']} with status {recipe_info['status']}")
            prompt = f"Professional food photography, 8k, {recipe_info['prompt']}"
            request_data = RequestData(prompt=prompt, api_key=stable_api)
            request = request_data.get_submit_dict()
            headers = {
                "apikey": request_data.api_key,
                "Client-Agent": request_data.client_agent,
            }
            new_recipe = recipe_info

            submit_req = requests.post("https://stablehorde.net/api/v2/generate/async", json=request, headers=headers)
            logging.info(f"submit_req: {submit_req}")
            if submit_req.ok:
                print("submit_req ok")
                submit_results = submit_req.json()
                req_id = submit_results['id']
                is_done = False
                cancelled = False
                retry = 0
                while not is_done:
                    try:
                        chk_req = requests.get(f'https://stablehorde.net/api/v2/generate/check/{req_id}')
                        if not chk_req.ok:
                            logging.info(chk_req.text)
                            break
                        chk_results = chk_req.json()
                        logging.info(chk_results)
                        is_done = chk_results['done']
                        time.sleep(0.8)
                    except ConnectionError as e:
                        retry += 1
                        logging.error(f"Error {e} when retrieving status. Retry {retry}/10")
                        if retry < 10:
                            time.sleep(1)
                            continue
                        raise
                if not cancelled:
                    retrieve_req = requests.get(f'https://stablehorde.net/api/v2/generate/status/{req_id}')
                    req_json = retrieve_req.json()
                    if req_json['generations']:
                        link = req_json['generations'][0]['img']
                        new_recipe['temp_image_url'] = link
                        new_recipe['status'] = "3_image_generated"
                        logging.info("Done generating image")
                        logging.info("Uploading image to S3")
                        
                        temp_url = link
                        region = "ap-southeast-2"
                        key = f"images/{recipe_info['url_name']}.png"
                        image_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{key}"
                        image_data = requests.get(temp_url).content

                        # Upload image to S3
                        try:
                            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)                            
                            s3.put_object(Bucket=bucket_name, Key=key, Body=image_data)
                        except Exception as e:
                            logging.error(f"Error: {e}")
                            raise
                        else:
                            logging.info("Image uploaded to S3 successfully!")
                            new_recipe['status'] = "4_image_uploaded"
                            new_recipe['image_url'] = image_url
                            update_mongo_val(recipe_info['_id'], 'status', '4_image_uploaded')
                            update_mongo_val(recipe_info['_id'], 'image_url', image_url)
                            logging.info(f"End: generate_recipe_image. Recipe '{recipe_info['name']}' has been successfully generated.")
                    else:
                        raise Exception("Image generation failed")
                else:
                    raise Exception("Image generation canceled")
            else:
                raise Exception("Image request failed")
        else:
            new_recipe = recipe_info
            logging.info(f"End: generate_recipe_image. Recipe '{recipe_info['name']}' has been already generated with status {recipe_info['status']}.")
        return new_recipe
    
    def get_recipe_embedding(tags: List[str]):
        client = boto3.client('lambda', region_name='ap-southeast-2', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        embeddings = []
        tags_single = [item for word in tags for item in word.split()]
        for tag in tags_single:
            input_data = {
                'tag': tag
            }

            response = client.invoke(
                FunctionName='getGloveEmbedding',
                Payload=json.dumps(input_data)
            )

            output_data = json.loads(response['Payload'].read())

            if 'statusCode' in output_data and output_data['statusCode'] == 200:
                logging.info("Success for tag: ", tag)
                embedding = output_data['body']
                embeddings.append(embedding)
                logging.debug(embedding)
            else:
                logging.error("Error for tag: ", tag)
                logging.error(output_data)
                # If embedding not found, skip it

        embed_sum = np.sum(embeddings, axis=0)
        embed_avg = embed_sum / len(embeddings)
        # Convert numpy array to list for json serialization
        emb_list = embed_avg.tolist()
        return emb_list
    
    @task(retries=3, retry_delay=timedelta(minutes=1))
    def calculate_embedding(recipe_info):
        if recipe_info['status'] == "4_image_uploaded":
            logging.info("Start: calculate_embedding. Calculating embedding for recipe: ", recipe_info['name'])
            try:
                tags = recipe_info['tags']
                embedding = get_recipe_embedding(tags)
            except Exception as e:
                logging.error(f"Error: {e}")
                raise
            else:
                new_recipe = recipe_info
                new_recipe['embedding'] = embedding
                new_recipe['status'] = "5_embedding_calculated"
                update_mongo_val(recipe_info['_id'], 'embedding', embedding)
                update_mongo_val(recipe_info['_id'], 'status', '5_embedding_calculated')
                logging.info(f"End: calculate_embedding. Recipe '{recipe_info['name']}' has been successfully generated.")
        else:
            new_recipe = recipe_info
            logging.info(f"End: calculate_embedding. Recipe '{recipe_info['name']}' has been already generated with status {recipe_info['status']}.")
        return new_recipe
    
    @task
    def upload_to_SQL(recipe_info):
        if recipe_info['status'] == "5_embedding_calculated":
            logging.info("Start: upload_to_SQL. Uploading recipe: ", recipe_info['name'])
            try:

                conn = psycopg2.connect(
                    host=rds_host,
                    database="postgres",
                    user=rds_user,
                    password=rds_password
                )

                cur = conn.cursor()
                postgres.insert_recipe(cur, recipe_info)
                
                conn.commit()
                cur.close()
                conn.close()

                recipe_info['status'] = "6_uploaded_to_SQL"
                update_mongo_val(recipe_info['_id'], 'status', '6_uploaded_to_SQL')
                logging.info(f"End: upload_to_SQL. Recipe '{recipe_info['name']}' has been successfully uploaded to SQL.")
            except Exception as e:
                logging.error(f"Error: {e}")
                raise
        else:
            logging.info(f"End: upload_to_SQL. Recipe '{recipe_info['name']}' has been already uploaded to SQL with status {recipe_info['status']}.")
        return recipe_info
    


    begin = EmptyOperator(task_id="begin")
    setup_task = setup()
    scrape_recipes_task = scrape_recipes()
    find_recipes_task = find_recipes()
    extract_from_mongo_tasks = extract_from_mongo.expand(recipe_id=find_recipes_task)
    chat_task = transform_chat.expand(recipe_info=extract_from_mongo_tasks)
    ingre_task = transform_ingredients.expand(recipe_info=chat_task)
    img_task = generate_recipe_image.expand(recipe_info=ingre_task)
    emb_task = calculate_embedding.expand(recipe_info=img_task)
    sql_task = upload_to_SQL.expand(recipe_info=emb_task)

    begin >> setup_task >> scrape_recipes_task >> find_recipes_task >> extract_from_mongo_tasks >> chat_task >> ingre_task >> img_task >> emb_task >> sql_task


recipe_etl()
