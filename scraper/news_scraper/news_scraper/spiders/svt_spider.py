import scrapy
from news_scraper.items import NewsScraperItem


class SVTSpider(scrapy.Spider):
    name = "svt"
    start_urls = [
        "https://www.svt.se/nyheter/ekonomi/",
        # 'https://www.svt.se/nyheter/svtforum/',
        # 'https://www.svt.se/nyheter/granskning/',
        # 'https://www.svt.se/nyheter/inrikes/',
        # 'https://www.svt.se/kultur/',
        # 'https://www.svt.se/nyheter/utrikes/'
    ]

    def parse(self, response):
        # Extract each article
        articles = response.xpath('//*[@id="innehall"]/div/section/ul//li/article/a')

        for article in articles:
            item = NewsScraperItem()

            item["title"] = article.xpath("@title").get()
            item["url"] = response.urljoin(article.xpath("@href").get())

            yield item
