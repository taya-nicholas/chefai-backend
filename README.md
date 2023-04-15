ChefAI is a free and open-source project that uses modern machine learning, MLOps, and data engineering practices to automate meal planning and cooking. You can check out the live website at [chefai.dev](https://chefai.dev).

# About

The project's backend (this repo) is written in Python and creates a data pipeline that scrapes recipes, applies ML technologies such as ChatGPT and Stable Diffusion to transform them, and then uploads them to an SQL database on AWS. The pipeline is orchestrated using Airflow, enabling scheduled scraping of new data and task retrying for unavailable API requests.

The [frontend](https://github.com/taya-nicholas/chefai-frontend), created with SvelteKit, connects to AWS RDS and S3 to serve the data in a responsive UI styled with Tailwind CSS. The recommendations section uses an AWS Lambda function to make an API call. The website also leverages local storage to keep track of saved items in the cart.

# Example

![Example of the Airflow DAG](/assets/DAG_example.png)

# Running

The backend is made out of several different components. Refer to each subfolder for detailed instructions for each of them.

# Future

Release 1.0 is just the beginning of the project, and in the future, the goal is to connect it to an online supermarket to pull food availability and prices. This would allow the recommender to generate plans for specific price ranges or diets. There are many other potential improvements that could be made, so if you have any ideas, please share them by creating a Github issue.

# Acknowledgments

The image generation in ChefAI is made possible thanks to StableHorde: https://github.com/db0/AI-Horde.
