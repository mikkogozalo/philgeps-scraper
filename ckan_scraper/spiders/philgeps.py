# -*- coding: utf-8 -*-
from urlparse import urljoin
import json

from scrapy.spider import Spider
from scrapy.http import Request

from ckan_scraper.items import CkanScraperItem


class RapplerSpider(Spider):
    name = 'philgeps'

    API_BASE = 'http://api.data.gov.ph/catalogue'
    RESOURCE_URL = (
        'http://api.data.gov.ph/catalogue/api/action/datastore_search'
        '?resource_id=%s&limit=1000'
    )
    RESOURCES = {
        'awards': '314aa773-e6e4-4554-80ce-4f588212e0d1',
        'bidders': '922f8c2c-8ef6-4e46-bc4e-8799c47b8ecf',
        'organization': '23de10e9-197e-4294-abd1-eba0188110cd',
        'bid_line_item': '63e29a04-6b13-48f8-ab13-ba72dc9ffcdc',
        # 'bid': '9c74991c-a5e6-4489-8413-c20a8a181d90',
        'location': '1da548c2-d141-4d9a-b159-8d36606d2ae2',
        'org_business': 'b0efc90c-88ab-469a-a694-e598dd47f724',
    }

    def start_requests(self):
        for resource, _id in self.RESOURCES.iteritems():
            yield Request(
                self.RESOURCE_URL % _id,
                self.parse_resources,
                meta={
                    'resource': resource
                }
            )

    def parse_resources(self, response):
        resource = response.meta['resource']
        res = json.loads(response.body)
        if res['success']:
            for record in res['result']['records']:
                item = CkanScraperItem()
                item['resource'] = resource
                item['contents'] = record
                yield item
            if 'next' in res['result']['_links']:
                yield Request(
                    self.API_BASE + res['result']['_links']['next'],
                    self.parse_resources,
                    meta={
                        'resource': resource
                    }
                )
