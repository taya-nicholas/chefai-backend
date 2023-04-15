def parse_transformed_recipe(recipe):
    """Currently deprecated due to the new recipe format """

    # Parse instructions
    instructions_start = recipe.index('Instructions:')
    instructions_end = recipe.index('Tags:')
    instructions = recipe[instructions_start:instructions_end].split('\n')[1:-1]
    if instructions[-1] == '':
        instructions.pop()

    # Parse tags
    tags_start = recipe.index('Tags:')
    tags_end = recipe.index('Description:')
    tags = recipe[tags_start:tags_end].split("\n")[1:-2][0].split(",")
        
    # Parse description
    description_start = recipe.index('Description:')
    description = recipe[description_start+len('Description:'):].strip()

    return instructions, tags, description