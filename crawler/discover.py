import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
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

FALLBACK_SEEDS = [
    "https://en.wikipedia.org/wiki/Special:Random",
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://developer.mozilla.org/en-US/",
    "https://www.freecodecamp.org/news/",
    "https://dev.to/",
    "https://stackoverflow.com/questions?tab=hot",
    "https://github.com/trending",
    "https://arstechnica.com/",
    "https://techcrunch.com/",
]

class DiscoverCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; DiscoverBot/1.0)"})
    
    def fetch(self, url):
        resp = self.session.get(url, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        final_url = resp.url
        
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        if not title:
            og = soup.find('meta', property='og:title')
            if og: title = og.get('content', '').strip()
        if not title: title = final_url
        
        domain = urlparse(final_url).netloc
        internal_links = []
        external_links = []
        
        for a in soup.find_all('a', href=True):
            full = urljoin(final_url, a['href'])
            parsed = urlparse(full)
            if parsed.scheme not in ('http', 'https'):
                continue
            path_lower = parsed.path.lower()
            if any(path_lower.endswith(ext) for ext in ['.jpg','.jpeg','.png','.gif','.svg','.css','.js','.pdf','.zip','.mp4','.mp3','.ico','.woff','.woff2']):
                continue
            if parsed.netloc == domain:
                internal_links.append(full)
            else:
                external_links.append(full)
        
        links = (internal_links[:5] + external_links[:3])
        return {"url": final_url, "title": title, "links": links}
    
    def get_crawled_urls(self):
        resp = requests.get(f"{PARSE_URL}/classes/Index", params={"order": "-createdAt", "limit": 1}, headers=HEADERS)
        if resp.status_code == 200:
            results = resp.json().get('results', [])
            if results and results[0].get('data'):
                try:
                    index_data = json.loads(results[0]['data'])
                    return set(index_data.get('urls', []))
                except: pass
        return set()
    
    def get_queue_count(self):
        """Check how many URLs are already in the queue"""
        where = json.dumps({"status": "pending"})
        resp = requests.get(f"{PARSE_URL}/classes/CrawlQueue", params={"where": where, "count": 1, "limit": 0}, headers=HEADERS)
        if resp.status_code == 200:
            return resp.json().get('count', 0)
        return 0
    
    def get_queue_urls(self):
        where = json.dumps({"status": "pending"})
        resp = requests.get(f"{PARSE_URL}/classes/CrawlQueue", params={"where": where, "limit": 300}, headers=HEADERS)
        if resp.status_code == 200:
            return set(item['url'] for item in resp.json().get('results', []))
        return set()
    
    def queue_url(self, url):
        resp = requests.post(f"{PARSE_URL}/classes/CrawlQueue", json={"url": url, "status": "pending"}, headers=HEADERS)
        return resp.status_code in [200, 201]
    
    def pick_seed_from_index(self, crawled, queue_urls):
        if len(crawled) < 5:
            return []
        crawled_list = list(crawled)
        picks = random.sample(crawled_list, min(3, len(crawled_list)))
        new_seeds = []
        for url in picks:
            try:
                page = self.fetch(url)
                for link in page['links']:
                    if link not in crawled and link not in queue_urls and link not in new_seeds:
                        new_seeds.append(link)
                        if len(new_seeds) >= 3:
                            break
                time.sleep(0.3)
            except Exception as e:
                print(f"  ⚠ Error: {e}")
        return new_seeds
    
    def discover(self):
        crawled = self.get_crawled_urls()
        queue_count = self.get_queue_count()
        queue_urls = self.get_queue_urls()
        all_known = crawled | queue_urls
        
        print(f"📚 {len(crawled)} indexed | 📋 {queue_count} in queue")
        
        # SMART THROTTLING: If queue already has 50+ URLs, add very few new ones
        if queue_count > 50:
            max_new = 5
            print("⚠ Queue is full! Adding only 5 new URLs to avoid backlog.")
        elif queue_count > 25:
            max_new = 10
            print("📋 Moderate queue. Adding 10 new URLs.")
        else:
            max_new = 20
            print("📋 Queue is low. Adding 20 new URLs.")
        
        new_found = 0
        
        # Strategy 1: Mine existing index
        print("\n🔍 Mining existing index for new links...")
        mined_seeds = self.pick_seed_from_index(crawled, all_known)
        for url in mined_seeds:
            if new_found >= max_new: break
            if url not in all_known:
                if self.queue_url(url):
                    print(f"  ✓ [mined] {url[:100]}")
                    all_known.add(url)
                    new_found += 1
        
        # Strategy 2: Fresh seeds (only if we still have room)
        if new_found < max_new:
            print(f"\n🌐 Exploring {min(2, len(FALLBACK_SEEDS))} fresh seeds...")
            seeds = random.sample(FALLBACK_SEEDS, min(2, len(FALLBACK_SEEDS)))
            for seed_url in seeds:
                if new_found >= max_new: break
                try:
                    page = self.fetch(seed_url)
                    if page['url'] not in all_known:
                        if self.queue_url(page['url']):
                            print(f"  ✓ [seed] {page['title'][:80]}")
                            all_known.add(page['url'])
                            new_found += 1
                    for link in page['links']:
                        if new_found >= max_new: break
                        if link not in all_known:
                            if self.queue_url(link):
                                print(f"  ✓ [link] {link[:100]}")
                                all_known.add(link)
                                new_found += 1
                    time.sleep(0.5)
                except Exception as e:
                    print(f"  ✗ {e}")
        
        # Strategy 3: Wikipedia random (only if desperate)
        if new_found < 3 and queue_count < 10:
            print(f"\n🎲 Random Wikipedia...")
            for _ in range(2):
                if new_found >= max_new: break
                try:
                    url = "https://en.wikipedia.org/wiki/Special:Random"
                    page = self.fetch(url)
                    if page['url'] not in all_known:
                        if self.queue_url(page['url']):
                            print(f"  ✓ [wiki] {page['title'][:80]}")
                            all_known.add(page['url'])
                            new_found += 1
                    time.sleep(0.3)
                except: pass
        
        print(f"\n✅ {new_found} new URLs queued ({queue_count + new_found} total in queue)")

if __name__ == "__main__":
    DiscoverCrawler().discover()
