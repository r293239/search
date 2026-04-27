import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import math
import time
import json
import random
from collections import defaultdict

PARSE_APP_ID = "qXJqQ3HWKYsGVB1oQKnYZo7zdNLHgjZMiwonhozr"
PARSE_REST_KEY = "mdTfymJLDHJY46HUv0tgKtWkqMm4YHQEbdsPX8tJ"
PARSE_URL = "https://parseapi.back4app.com"

HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
}

# Random exploration seeds — the crawler picks from these to find new stuff
EXPLORE_SEEDS = [
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://developer.mozilla.org/en-US/docs/Web/JavaScript",
    "https://www.freecodecamp.org/news/",
    "https://dev.to/",
    "https://stackoverflow.com/questions?tab=hot",
    "https://github.com/trending",
    "https://medium.com/tag/programming",
    "https://en.wikipedia.org/wiki/Internet",
    "https://en.wikipedia.org/wiki/Computer_science",
]


class DiscoverCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DiscoverBot/1.0)"})
    
    def fetch(self, url):
        """Download and parse a webpage"""
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        title = soup.title.string.strip() if soup.title else url
        text = soup.get_text(separator=' ', strip=True)[:5000]
        links = []
        for a in soup.find_all('a', href=True):
            full = urljoin(url, a['href'])
            parsed = urlparse(full)
            # Only keep interesting links (skip images, scripts, etc)
            if parsed.scheme in ('http', 'https') and not full.endswith(('.jpg','.png','.gif','.css','.js','.pdf','.zip')):
                links.append(full)
        return {"url": url, "title": title, "text": text, "links": links[:15]}
    
    def get_crawled_urls(self):
        """Get list of already-crawled URLs from index"""
        resp = requests.get(
            f"{PARSE_URL}/classes/Index",
            params={"order": "-createdAt", "limit": 1},
            headers=HEADERS
        )
        if resp.status_code == 200:
            results = resp.json().get('results', [])
            if results and results[0].get('data'):
                try:
                    index_data = json.loads(results[0]['data'])
                    return set(index_data.get('urls', []))
                except:
                    pass
        return set()
    
    def queue_url(self, url):
        """Add URL to crawl queue if not already queued or crawled"""
        # Check if already in queue
        where = json.dumps({"url": url, "status": "pending"})
        check = requests.get(
            f"{PARSE_URL}/classes/CrawlQueue",
            params={"where": where, "count": 1, "limit": 1},
            headers=HEADERS
        )
        if check.status_code == 200 and check.json().get('count', 0) > 0:
            return False
        
        # Add to queue
        resp = requests.post(
            f"{PARSE_URL}/classes/CrawlQueue",
            json={"url": url, "status": "pending"},
            headers=HEADERS
        )
        return resp.status_code in [200, 201]
    
    def discover(self):
        """Main discovery routine"""
        crawled = self.get_crawled_urls()
        print(f"Currently have {len(crawled)} pages indexed")
        
        new_found = 0
        
        # Pick 2 random exploration seeds
        explore_urls = random.sample(EXPLORE_SEEDS, min(2, len(EXPLORE_SEEDS)))
        
        for seed_url in explore_urls:
            try:
                print(f"🔍 Exploring: {seed_url}")
                page = self.fetch(seed_url)
                
                # Queue the seed page if not already crawled
                if page['url'] not in crawled:
                    if self.queue_url(page['url']):
                        print(f"  ✓ Queued seed: {page['url']}")
                        new_found += 1
                
                # Queue discovered links that aren't crawled yet
                new_links = [l for l in page['links'] if l not in crawled][:5]
                for link in new_links:
                    if self.queue_url(link):
                        print(f"  ✓ Queued: {link}")
                        new_found += 1
                
                time.sleep(1)  # Be polite
                
            except Exception as e:
                print(f"  ✗ Failed: {e}")
        
        print(f"\n✅ Discovery done! {new_found} new URLs queued.")
        
        # If we found new stuff, trigger main crawler? (can't trigger other workflows easily)
        if new_found > 0:
            print("💡 New URLs waiting! Run the main crawler to index them.")


if __name__ == "__main__":
    crawler = DiscoverCrawler()
    crawler.discover()
