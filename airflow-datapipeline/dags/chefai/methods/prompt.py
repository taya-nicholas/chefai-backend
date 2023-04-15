QUESTION_TEMPLATE = """For this recipe do three things.
First, rewrite the instructions to include ingredient quantities.
Second, generate 5 relevant tags for this recipe, each a single word. Pay attention to the type of meat, carbohydrate, and style.
Third, generate a brief, approximately 35 word description of this recipe.

Your answer should be in the following format.

Instructions:
1. First instruction.
2. Second instruction.

Tags:
First-tag, second-tag

Description:
Description of the recipe.
"""

TAGS_TEMPLATE = """For this recipe do the following:
            - Generate 5 tags, using single words for each.
            - Generate a brief 35 word description.
            - Generate a description to be used as a prompt to generate a nice looking image.
            - Create a name for the recipe. 

            Your response should be in the form:

            Tags:
            tag1, tag2, tag3, tag4, tag5

            Description:
            description here

            Prompt:
            image prompt here

            Name:
            name here
            """

INSTRUCTIONS_TEMPLATE = """
            Rewrite the instructions in a clear style that combines the information from ingredients and instructions. Ensure that ingredient quantities are included in the instructions.
            Return you response in the form:

            Instructions:
            instructions go here.
            """
