
class RequestData(object):
    def __init__(self, prompt, api_key="0000000000"):
            self.client_agent = "recipe_etl:1.0.0"
            self.api_key = api_key
            self.imgen_params = {
                "n": 1,
                "width": 768,
                "height":768,
                "steps": 30,
                "sampler_name": "k_euler_a",
                "cfg_scale": 9,
                "denoising_strength": 0.6,
                "post_processing": [
                    
                ],
            }
            self.submit_dict = {
                "prompt": prompt,
                "api_key": api_key,
                "nsfw": False,
                "censor_nsfw": False,
                "trusted_workers": False,
                "models": ["Realistic Vision"],
                "r2": True
            }
            self.source_image = None
            self.source_processing = "img2img"
            self.source_mask = None

    def get_submit_dict(self):
        submit_dict = self.submit_dict.copy()
        submit_dict["params"] = self.imgen_params
        submit_dict["source_processing"] = self.source_processing
        return(submit_dict)