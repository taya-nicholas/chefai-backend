# About

This lambda function uses a GloVe model to generate embeddings for each recipe, which are used by the recommendation utility in the data pipeline.

# How it works

The function downloads the GloVe model from S3, using Gensim, and then calculates the embedding for a specified tag. The resulting embedding is returned.

# Performance

To minimize download time, the model is loaded once per warm-up period, which takes approximately 3000 ms. After that, each embedding calculation takes only 1-2 ms.

# Configuration

The memory has been increased to 512 MB, and a lambda layer with Gensim installed has been added. All other parameters remain unchanged.
