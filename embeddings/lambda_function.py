import json
from gensim.models import KeyedVectors
import boto3

s3 = boto3.client("s3")
bucket_name = 'chefai'
prefix = 'models/'

filename_model = 'glove-wiki-gigaword-100.model'
s3_path_model = prefix + filename_model

filename_npy = 'glove-wiki-gigaword-100.model.vectors.npy'
s3_path_npy = prefix + filename_npy

s3.download_file(bucket_name, s3_path_model, '/tmp/glove-wiki-gigaword-100.model')
s3.download_file(bucket_name, s3_path_npy, '/tmp/glove-wiki-gigaword-100.model.vectors.npy')
glove_vectors = KeyedVectors.load('/tmp/glove-wiki-gigaword-100.model')


def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': glove_vectors[event['tag']].tolist()
    }
