from bs4 import BeautifulSoup
import requests
import logging
import sys



class RecipeScraper:

    def __init__(self, collection, meta):
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)
        logging.info("Recipe scrapper init start")

        self.collection = collection
        self.meta = meta

        # use meta mongo connection to get doc with status = 'visited'
        links = self.meta.find_one({'status': 'visit_links'})
        if links:
            links = links['visited_links']
        else:
            blank = {
                'status': 'visit_links',
                'visited_links': []
            }
            self.meta.insert_one(blank)
            links = []
        self.visited_links = set(links)

        self.session = requests.Session()
        self.base_url = "https://www.recipetineats.com/category/main-dishes/"
        logging.info("Recipe scrapper init end")

    def run(self, page_limit):
        logging.info("Starting scraping")
        for page_number in range(1, page_limit+1):
            index_page_url = self.base_url if page_number == 1 else f"{self.base_url}page/{page_number}/"
            self._scrap_index(index_page_url)
        self._finish()
        logging.info("Scraping run done")

    def _scrap_index(self, index_page_url):
        # Fetch the HTML source code of the index page
        index_page_response = self.session.get(index_page_url)
        index_page_soup = BeautifulSoup(
            index_page_response.content, "html.parser")

        main_tag = index_page_soup.find("main")
        recipe_links = main_tag.find_all("a", href=True)

        # Loop through each recipe link
        for link in recipe_links:
            self._scrap_recipe(link)

    def _scrap_recipe(self, link):
        recipe_page_url = link["href"]
        url_name = recipe_page_url.split("/")[-2]

        if "category/main-dishes" in recipe_page_url:
            return

        if recipe_page_url in self.visited_links:
            return

        self.visited_links.add(recipe_page_url)
        logging.info("Current link: %s", link["href"])

        response = self.session.get(recipe_page_url)
        soup = BeautifulSoup(response.content, "html.parser")

        recipe_name = self._get_recipe_name(soup)
        ingredient_list = self._get_ingredient_list(soup)
        instruction_list = self._get_instruction_list(soup)

        recipe = {
            "name": recipe_name,
            "url_name": url_name,
            "url": recipe_page_url,
            "ingredients": ingredient_list,
            "instructions": instruction_list,
            "status": "unprocessed"
        }

        # Insert the recipe into the "recipe" collection
        self.collection.insert_one(recipe)

    def _finish(self):
        logging.info("Shutting down scraping")
        # updated visited links in mongo
        self._update_mono_links(list(self.visited_links))
        logging.info("Successfully shut down")

    def _update_mono_links(self, links):
        document = self.meta.find_one({'status': 'visit_links'})
        self.meta.update_one({'_id': document['_id']}, {'$set': {'visited_links': links}})

    def _get_text(self, soup_object):
        """
        Returns the text of a soup object after stripping leading/trailing whitespace and commas.
        """
        if soup_object is None:
            return ""
        elif isinstance(soup_object, str) and soup_object.isspace():
            return ""
        else:
            return soup_object.text.strip().strip(",")

    def _get_recipe_name(self, recipe_soup):
        """
        Returns the name of the recipe from the recipe soup object.
        """
        return recipe_soup.find("h1", class_="entry-title").text

    def _get_ingredient_list(self, recipe_soup):
        """
        Returns a list of dictionaries representing the ingredients of the recipe from the recipe soup object.
        """
        final_ingredients = []
        ingredient_groups = recipe_soup.find_all(
            'div', {'class': 'wprm-recipe-ingredient-group'})
        for group in ingredient_groups:
            group_name = self._get_text(group.find(
                "h4", {"class": "wprm-recipe-group-name wprm-recipe-ingredient-group-name wprm-block-text-bold"}))
            ingredients = [
                {
                    "amount": self._get_text(ingredient.find("span", {"class": "wprm-recipe-ingredient-amount"})),
                    "unit": self._get_text(ingredient.find("span", {"class": "wprm-recipe-ingredient-unit"})),
                    "name": self._get_text(ingredient.find("span", {"class": "wprm-recipe-ingredient-name"})),
                    "notes": self._get_text(ingredient.find("span", {"class": "wprm-recipe-ingredient-notes wprm-recipe-ingredient-notes-faded"}))
                } for ingredient in group.find_all('li', {'class': 'wprm-recipe-ingredient'})
            ]

            final_ingredients.append({
                "group_name": group_name,
                "ingredients": ingredients
            })

        return final_ingredients

    def _get_instruction_list(self, recipe_soup):
        """
        Returns a list of dictionaries representing the instructions of the recipe from the recipe soup object.
        """
        final_instructions = []
        instruction_groups = recipe_soup.find_all(
            'div', {'class': 'wprm-recipe-instruction-group'})
        for group in instruction_groups:
            group_name = self._get_text(group.find(
                "h4", {"class": "wprm-recipe-group-name wprm-recipe-instruction-group-name wprm-block-text-bold"}))
            instructions = [
                self._get_text(instruction) for instruction in group.find_all('div', {'class': 'wprm-recipe-instruction-text'})
            ]

            final_instructions.append({
                "group_name": group_name,
                "instructions": instructions
            })

        return final_instructions
