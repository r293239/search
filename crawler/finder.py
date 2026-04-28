"""
Web Discovery Crawler - Finds new domains and URLs organically by following links
No hardcoded seed lists. Starts from the index and expands outward.
"""
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

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SearchBot/1.0)"
}

# Only used as bootstrap if index is completely empty
BOOTSTRAP_URLS = [
    "https://en.wikipedia.org/wiki/World_Wide_Web",
    "https://en.wikipedia.org/wiki/Internet",
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://github.com/",
]

class WebFinder:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(FETCH_HEADERS)
        self.new_urls = set()
        self.new_domains = set()
        self.crawled_urls = set()
        self.queued_urls = set()
    
    def load_state(self):
        """Load current index and queue to avoid duplicates"""
        # Get indexed URLs
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
                    self.crawled_urls = set(index_data.get('urls', []))
                except:
                    pass
        
        # Get queued URLs
        where = json.dumps({"status": "pending"})
        resp = requests.get(
            f"{PARSE_URL}/classes/CrawlQueue",
            params={"where": where, "limit": 500},
            headers=HEADERS
        )
        if resp.status_code == 200:
            for item in resp.json().get('results', []):
                self.queued_urls.add(item['url'])
        
        print(f"📚 {len(self.crawled_urls)} indexed | 📋 {len(self.queued_urls)} queued")
    
    def fetch_page(self, url):
        """Fetch a page and extract all outbound links"""
        try:
            resp = self.session.get(url, timeout=15, allow_redirects=True)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            final_url = resp.url
            
            links = []
            for a in soup.find_all('a', href=True):
                full = urljoin(final_url, a['href'])
                parsed = urlparse(full)
                
                # Skip non-http, same-page anchors, mailto, tel
                if parsed.scheme not in ('http', 'https'):
                    continue
                if not parsed.netloc:
                    continue
                
                # Skip obvious non-content files
                path = parsed.path.lower()
                skip_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.css', 
                                   '.js', '.pdf', '.zip', '.mp4', '.mp3', '.ico', 
                                   '.woff', '.woff2', '.ttf', '.xml', '.rss', '.json']
                if any(path.endswith(ext) for ext in skip_extensions):
                    continue
                
                # Normalize URL (remove fragment)
                clean_url = full.split('#')[0]
                links.append(clean_url)
            
            return links
        except Exception as e:
            return []
    
    def expand_from_url(self, url):
        """Visit a URL and extract all new links from it"""
        if url in self.crawled_urls or url in self.queued_urls:
            return []
        
        print(f"  🌐 Exploring: {url[:120]}")
        links = self.fetch_page(url)
        
        new_links = []
        for link in links:
            if link not in self.crawled_urls and link not in self.queued_urls and link not in self.new_urls:
                new_links.append(link)
                self.new_urls.add(link)
                
                domain = urlparse(link).netloc
                if domain not in self.new_domains:
                    self.new_domains.add(domain)
        
        if new_links:
            print(f"    → Found {len(new_links)} new links (from {len(links)} total)")
        
        time.sleep(0.5)
        return new_links
    
    def find_from_index(self, count=5):
        """Pick random pages from the index and follow their links outward"""
        if len(self.crawled_urls) < 5:
            return []
        
        crawled_list = list(self.crawled_urls)
        picks = random.sample(crawled_list, min(count, len(crawled_list)))
        
        all_new = []
        for url in picks:
            new = self.expand_from_url(url)
            all_new.extend(new)
        
        return all_new
    
    def find_from_external_domains(self, max_domains=10):
        """Look at the domains we've found and try their homepages"""
        if not self.new_domains:
            return []
        
        domains = list(self.new_domains)
        random.shuffle(domains)
        
        all_new = []
        for domain in domains[:max_domains]:
            # Try the homepage
            for scheme in ['https://', 'http://']:
                homepage = f"{scheme}{domain}/"
                if homepage not in self.crawled_urls and homepage not in self.queued_urls:
                    new = self.expand_from_url(homepage)
                    all_new.extend(new)
                    break
            
            # Try common paths
            common_paths = ['/blog', '/news', '/articles', '/posts', '/about', '/wiki']
            path = random.choice(common_paths)
            path_url = f"https://{domain}{path}"
            if path_url not in self.crawled_urls and path_url not in self.queued_urls:
                new = self.expand_from_url(path_url)
                all_new.extend(new)
        
        return all_new
    
    def bootstrap_if_empty(self):
        """If index is completely empty, start from bootstrap URLs"""
        if len(self.crawled_urls) > 0:
            return []
        
        print("🌱 Index is empty! Bootstrapping from seed URLs...")
        all_new = []
        for url in BOOTSTRAP_URLS[:3]:
            new = self.expand_from_url(url)
            all_new.extend(new)
        
        return all_new
    
    def queue_all_found(self):
        """Add all discovered URLs to the crawl queue"""
        urls_to_queue = list(self.new_urls)[:200]  # Max 200 per run
        
        count = 0
        for url in urls_to_queue:
            if url in self.queued_urls:
                continue
            try:
                resp = requests.post(
                    f"{PARSE_URL}/classes/CrawlQueue",
                    json={"url": url, "status": "pending"},
                    headers=HEADERS
                )
                if resp.status_code in [200, 201]:
                    count += 1
                    self.queued_urls.add(url)
            except:
                pass
        
        return count
    
    def run(self):
        """Main discovery routine"""
        print("=" * 50)
        print("🔍 WEB FINDER - Organic Discovery")
        print("=" * 50)
        
        self.load_state()
        
        total_found = 0
        
        # Step 1: Bootstrap if empty
        print("\n📍 Step 1: Bootstrap check...")
        bootstrapped = self.bootstrap_if_empty()
        if bootstrapped:
            total_found += len(bootstrapped)
            print(f"  Bootstrapped {len(bootstrapped)} URLs")
        
        # Step 2: Expand from existing index pages
        print("\n🔗 Step 2: Expanding from indexed pages...")
        expanded = self.find_from_index(count=5)
        total_found += len(expanded)
        print(f"  Found {len(expanded)} new URLs from existing index")
        
        # Step 3: Explore newly discovered domains
        if self.new_domains:
            print(f"\n🌍 Step 3: Exploring {min(10, len(self.new_domains))} newly discovered domains...")
            domain_links = self.find_from_external_domains(max_domains=10)
            total_found += len(domain_links)
            print(f"  Found {len(domain_links)} new URLs from new domains")
        
        # Step 4: Deep dive - pick some new URLs and explore them too
        if self.new_urls and total_found < 50:
            print(f"\n🔬 Step 4: Deep diving into discovered URLs...")
            sample = list(self.new_urls)
            random.shuffle(sample)
            for url in sample[:3]:
                new = self.expand_from_url(url)
                total_found += len(new)
        
        # Queue everything
        print("\n📋 Queuing discovered URLs...")
        queued = self.queue_all_found()
        
        print("\n" + "=" * 50)
        print(f"📊 SUMMARY")
        print(f"   New URLs found: {len(self.new_urls)}")
        print(f"   New domains discovered: {len(self.new_domains)}")
        print(f"   URLs queued for crawling: {queued}")
        print(f"   Total indexed: {len(self.crawled_urls)}")
        print(f"   Total in queue: {len(self.queued_urls)}")
        print("=" * 50)


if __name__ == "__main__":
    finder = WebFinder()
    finder.run()
