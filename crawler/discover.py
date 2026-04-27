import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import time
import json
import random

PARSE_APP_ID = "qXJqQ3HWKYsGVB1oQKnYZo7zdNLHgjZMiwonhozr"
PARSE_REST_KEY = "mdTfymJLDHJY46HUv0tgKtWkqMm4YHQEbdsPX8tJ"
PARSE_URL = "https://parseapi.back4app.com"

HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
}

EXPLORE_SEEDS = [
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://en.wikipedia.org/wiki/Special:Random",
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
    "https://en.wikipedia.org/wiki/Artificial_intelligence",
    "https://www.theverge.com/",
    "https://arstechnica.com/",
    "https://techcrunch.com/",
    "https://www.wired.com/",
]


class DiscoverCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DiscoverBot/1.0)"})
    
    def fetch(self, url):
        """Download a page and extract metadata + links"""
        resp = self.session.get(url, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # Get final URL after redirects (important for Wikipedia Special:Random)
        final_url = resp.url
        
        # Title
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        if not title:
            og_title = soup.find('meta', property='og:title')
            if og_title:
                title = og_title.get('content', '').strip()
        if not title:
            title = final_url
        
        # Description
        description = ''
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            description = meta_desc['content'].strip()
        
        if not description:
            og_desc = soup.find('meta', property='og:description')
            if og_desc and og_desc.get('content'):
                description = og_desc['content'].strip()
        
        if not description:
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript']):
                tag.decompose()
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(separator=' ', strip=True)
                if len(text) > 100 and not any(skip in text.lower()[:50] for skip in 
                    ['cookie', 'accept', 'subscribe', 'sign up', 'log in', '©', 'all rights reserved', 'menu']):
                    description = text
                    break
        
        if not description:
            body_text = soup.get_text(separator=' ', strip=True)
            description = body_text[:300]
        
        description = ' '.join(description.split())[:500]
        
        # Extract links
        links = []
        for a in soup.find_all('a', href=True):
            full = urljoin(final_url, a['href'])
            parsed = urlparse(full)
            if parsed.scheme in ('http', 'https'):
                # Skip non-content files
                path_lower = parsed.path.lower()
                if not any(path_lower.endswith(ext) for ext in ['.jpg','.jpeg','.png','.gif','.svg','.css','.js','.pdf','.zip','.mp4','.mp3','.ico','.woff','.woff2']):
                    links.append(full)
        
        return {
            "url": final_url, 
            "title": title, 
            "description": description,
            "links": links[:15]
        }
    
    def get_crawled_urls(self):
        """Get list of already-crawled URLs from the index"""
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
        """Add URL to crawl queue if not already there"""
        # Check if already pending
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
        print(f"📚 Currently have {len(crawled)} pages indexed")
        
        new_found = 0
        max_new = 10  # Don't add too many at once
        
        # Pick 3 random exploration seeds
        explore_urls = random.sample(EXPLORE_SEEDS, min(3, len(EXPLORE_SEEDS)))
        
        for seed_url in explore_urls:
            if new_found >= max_new:
                break
            
            try:
                print(f"🔍 Exploring: {seed_url}")
                page = self.fetch(seed_url)
                
                # Queue the seed page if not already crawled
                if page['url'] not in crawled:
                    if self.queue_url(page['url']):
                        print(f"  ✓ Queued: {page['title'][:80]}")
                        crawled.add(page['url'])
                        new_found += 1
                
                # Queue discovered links
                new_links = [l for l in page['links'] if l not in crawled]
                for link in new_links:
                    if new_found >= max_new:
                        break
                    if self.queue_url(link):
                        print(f"  ✓ Queued: {link[:100]}")
                        crawled.add(link)
                        new_found += 1
                
                if new_found > 0:
                    time.sleep(2)
                
            except Exception as e:
                print(f"  ✗ Failed: {e}")
        
        print(f"\n✅ Discovery done! {new_found} new URLs queued.")
        
        if new_found > 0:
            print("💡 Run the main crawler workflow to index these new pages!")
        else:
            print("📭 No new URLs found this round. The index is growing nicely!")


if __name__ == "__main__":
    crawler = DiscoverCrawler()
    crawler.discover()
