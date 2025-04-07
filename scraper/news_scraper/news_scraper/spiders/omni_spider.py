import scrapy

class OmniSpider(scrapy.Spider):
    name = "omni"
    allowed_domains = ["omni.se"]
    start_urls = [
        "https://www.omni.se/senaste"
    ]

    def __init__(self, *args, **kwargs):
        super(OmniSpider).__init__(*args, **kwargs)

        self.website_name = "Omni"
        self.website_url = "https://omni.se"
    
    def parse(self, response):
        pass