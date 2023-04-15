# About
This AWS Lambda function generates a diverse meal plan with a variety of styles, tastes, and ingredients. It takes two parameters: num_recipes (minimum 2) and iterations (number of loops used during generation).

# How it works
The function generates a random set of recipe IDs and calculates the meal plan score by retrieving the embedding of each recipe from an AWS RDS instance. It then calculates the cosine similarity between all unique recipe pairs and averages the result. A lower cosine score indicates greater meal diversity.

This process is repeated for n iterations to introduce randomness in the generated meal plans. Increasing the number of iterations decreases the average score, allowing for more diversity. Although it's possible to generate a meal plan with a minimum similarity score, doing so would result in similar or identical recipes each time. The iterations parameter enables users to tune the degree of randomness and variety in their meal plans.

# Performance
The set of available IDs is calculated during warm-up, taking roughly 750ms. Subsequent requests' performance varies based on the parameters used:


| Num Recipes | Iterations | Time (ms) |
| ----------- | ---------- | --------- |
| 4           | 3          | 7         |
| 4           | 8          | 50        |
| 4           | 12         | 90        |
| 7           | 3          | 40        |
| 7           | 8          | 110       |
| 7           | 12         | 190       |


While execution time increases rapidly, practical use cases require generating only 2-7 recipes. I've found that 3-4 iterations provide sufficient diversity without frequently repeating the same local minima. As new features are added, however, optimization may become necessary.

# Configuration
All configuration parameters remain unchanged. I've defined three environment variables (host_name, password, and user_name) to connect to the AWS RDS PostgreSQL server containing recipe embeddings. I've also created a Lambda layer that installs the numpy and psycopg2 packages.