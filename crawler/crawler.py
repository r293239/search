import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import math
import time
import json
from collections import defaultdict

PARSE_APP_ID = "qXJqQ3HWKYsGVB1oQKnYZo7zdNLHgjZMiwonhozr"
PARSE_REST_KEY = "mdTfymJLDHJY46HUv0tgKtWkqMm4YHQEbdsPX8tJ"
PARSE_URL = "https://parseapi.back4app.com"

HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
}

class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SearchBot/1.0)"})
    
    def fetch(self, url):
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
        if not title:
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                title = og_title['content'].strip()
        if not title:
            title = url
        
        description = ''
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            description = meta_desc['content'].strip()
        if not description:
            og_desc = soup.find('meta', property='og:description')
            if og_desc and og_desc.get('content'):
                description = og_desc['content'].strip()
        if not description:
            for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'noscript', 'iframe']):
                tag.decompose()
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text(separator=' ', strip=True)
                if len(text) > 100 and not any(skip in text.lower()[:50] for skip in 
                    ['cookie', 'accept', 'subscribe', 'sign up', 'log in', '©', 'all rights reserved', 'menu', 'navigation']):
                    description = text
                    break
        if not description:
            for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
                tag.decompose()
            body_text = soup.get_text(separator=' ', strip=True)
            description = body_text[:300]
        description = ' '.join(description.split())[:500]
        
        full_text = soup.get_text(separator=' ', strip=True)[:10000]
        
        links = []
        for a in soup.find_all('a', href=True):
            full = urljoin(url, a['href'])
            if full.startswith('http'):
                links.append(full)
        
        return {"url": url, "title": title, "description": description, "text": full_text, "links": links[:20]}
    
    def crawl(self, start_urls, max_pages=200):
        visited = set()
        queue = list(start_urls)
        pages = []
        while queue and len(pages) < max_pages:
            url = queue.pop(0)
            if url in visited:
                continue
            visited.add(url)
            try:
                page = self.fetch(url)
                pages.append(page)
                # Add discovered links to the crawl queue (keeps going deeper)
                new_links = [l for l in page['links'] if l not in visited and l not in queue]
                queue.extend(new_links[:5])  # Add up to 5 new links per page
                print(f"✓ [{len(pages)}/{max_pages}] {url}")
                time.sleep(0.5)  # Faster crawling (was 1 second)
            except Exception as e:
                print(f"✗ {url}: {e}")
        return pages

class Indexer:
    def __init__(self):
        self.stopwords = {"the","a","an","is","are","was","were","be","been","being","have","has","had","do","does","did","will","would","shall","should","may","might","must","can","could","of","in","to","for","with","on","at","by","from","and","or","not","but","if","then","else","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","only","own","same","so","than","too","very","this","that","it","its","he","she","they","them","these","those","i","my","your","we","our","you","me","us","him","his","her","their"}
    
    def tokenize(self, text):
        words = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())
        return [w for w in words if w not in self.stopwords]
    
    def build_index(self, pages):
        N = len(pages)
        doc_tokens = {}
        doc_urls = []
        for i, page in enumerate(pages):
            tokens = self.tokenize(page['text'])
            doc_tokens[i] = tokens
            doc_urls.append(page['url'])
        df = defaultdict(int)
        for tokens in doc_tokens.values():
            for word in set(tokens):
                df[word] += 1
        index = {}
        for i, tokens in doc_tokens.items():
            tf = defaultdict(int)
            for w in tokens:
                tf[w] += 1
            max_tf = max(tf.values()) if tf else 1
            for word, count in tf.items():
                if word not in index:
                    index[word] = {}
                tf_norm = count / max_tf
                idf = math.log((N - df[word] + 0.5) / (df[word] + 0.5) + 1)
                index[word][str(i)] = round(tf_norm * idf, 4)
        return {
            "index": index,
            "urls": doc_urls,
            "titles": [p.get('title', 'Untitled') for p in pages],
            "snippets": [p.get('description', p['text'][:300]) for p in pages],
            "doc_count": N,
            "timestamp": time.time()
        }
    
    def save_to_back4app(self, index_data):
        # Append to existing index instead of replacing
        existing_pages = self.get_existing_pages()
        
        payload = {"data": json.dumps(index_data), "docCount": index_data["doc_count"], "timestamp": int(index_data["timestamp"])}
        resp = requests.post(f"{PARSE_URL}/classes/Index", json=payload, headers=HEADERS)
        if resp.status_code in [200, 201]:
            print(f"✅ Index saved! {index_data['doc_count']} docs, {len(index_data['index'])} terms")
            if existing_pages > 0:
                print(f"   (Previous index had {existing_pages} pages - old indexes kept as backup)")
            return True
        else:
            print(f"❌ Failed: {resp.text}")
            return False
    
    def get_existing_pages(self):
        """Check how many pages are in the latest index"""
        resp = requests.get(f"{PARSE_URL}/classes/Index", params={"order": "-createdAt", "limit": 1}, headers=HEADERS)
        if resp.status_code == 200:
            results = resp.json().get('results', [])
            if results and results[0].get('data'):
                try:
                    idx = json.loads(results[0]['data'])
                    return idx.get('doc_count', 0)
                except:
                    pass
        return 0

def get_all_queue():
    """Get ALL pending URLs from queue (not just 10)"""
    where = json.dumps({"status": "pending"})
    resp = requests.get(f"{PARSE_URL}/classes/CrawlQueue", params={"where": where, "limit": 200}, headers=HEADERS)
    if resp.status_code == 200:
        return [item for item in resp.json().get('results', [])]
    return []

def delete_queue_items(objectIds):
    """Batch delete queue items"""
    for oid in objectIds:
        requests.delete(f"{PARSE_URL}/classes/CrawlQueue/{oid}", headers=HEADERS)

def main():
    crawler = Crawler()
    indexer = Indexer()
    
    queue_items = get_all_queue()
    
    if not queue_items:
        print("✅ No URLs in queue. Everything is indexed!")
        return
    
    # Take ALL queue URLs (up to 100) as seeds
    seeds = [item['url'] for item in queue_items[:100]]
    queue_ids = [item['objectId'] for item in queue_items[:100]]
    
    print(f"📋 {len(queue_items)} URLs in queue")
    print(f"🕷️ Crawling {len(seeds)} seeds, up to 200 pages total...")
    print(f"⚡ Speed: 0.5s delay between requests\n")
    
    pages = crawler.crawl(seeds, max_pages=200)
    
    if pages:
        print(f"\n📊 Building index from {len(pages)} pages...")
        index = indexer.build_index(pages)
        
        print("💾 Saving to Back4App...")
        if indexer.save_to_back4app(index):
            # Delete processed queue items
            delete_queue_items(queue_ids)
            
            # Report on remaining queue
            remaining = len(queue_items) - len(queue_ids)
            if remaining > 0:
                print(f"📋 {remaining} URLs still in queue for next run")
            else:
                print("✅ Queue fully cleared!")
    else:
        print("❌ No pages crawled.")

if __name__ == "__main__":
    main()
