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
        try:
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
            
            return {"url": url, "title": title, "description": description, "text": full_text, "success": True}
        except Exception as e:
            return {"url": url, "title": url, "description": "", "text": "", "success": False, "error": str(e)}
    
    def crawl(self, urls):
        """Crawl all URLs provided, no artificial limit"""
        pages = []
        total = len(urls)
        
        for i, url in enumerate(urls):
            page = self.fetch(url)
            if page['success']:
                pages.append(page)
                print(f"✓ [{len(pages)}/{total}] {url}")
            else:
                print(f"✗ [{i+1}/{total}] {url} - {page.get('error', 'failed')}")
            
            # Small delay to be polite to servers
            if i < total - 1:
                time.sleep(0.3)
        
        return pages


class Indexer:
    def __init__(self):
        self.stopwords = {"the","a","an","is","are","was","were","be","been","being","have","has","had","do","does","did","will","would","shall","should","may","might","must","can","could","of","in","to","for","with","on","at","by","from","and","or","not","but","if","then","else","when","where","why","how","all","any","both","each","few","more","most","other","some","such","no","only","own","same","so","than","too","very","this","that","it","its","he","she","they","them","these","those","i","my","your","we","our","you","me","us","him","his","her","their"}
    
    def tokenize(self, text):
        words = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())
        return [w for w in words if w not in self.stopwords]
    
    def build_index(self, pages):
        if not pages:
            return None
            
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
        payload = {
            "data": json.dumps(index_data),
            "docCount": index_data["doc_count"],
            "timestamp": int(index_data["timestamp"])
        }
        resp = requests.post(f"{PARSE_URL}/classes/Index", json=payload, headers=HEADERS)
        if resp.status_code in [200, 201]:
            print(f"✅ Index saved! {index_data['doc_count']} docs, {len(index_data['index'])} terms")
            return True
        else:
            print(f"❌ Failed: {resp.text}")
            return False


def get_all_queue():
    """Get ALL pending URLs from the queue"""
    where = json.dumps({"status": "pending"})
    resp = requests.get(f"{PARSE_URL}/classes/CrawlQueue", params={"where": where, "limit": 500}, headers=HEADERS)
    if resp.status_code == 200:
        return resp.json().get('results', [])
    return []


def delete_queue_items(objectIds):
    """Delete processed queue items in batches"""
    batch_size = 50
    for i in range(0, len(objectIds), batch_size):
        batch = objectIds[i:i+batch_size]
        for oid in batch:
            requests.delete(f"{PARSE_URL}/classes/CrawlQueue/{oid}", headers=HEADERS)
        print(f"  🗑 Deleted {len(batch)} queue items")


def main():
    crawler = Crawler()
    indexer = Indexer()
    
    # Get everything from queue
    queue_items = get_all_queue()
    
    if not queue_items:
        print("✅ Queue is empty! Nothing to crawl.")
        return
    
    total_in_queue = len(queue_items)
    print(f"📋 {total_in_queue} URLs in queue")
    print(f"🕷️ Starting crawl on all {total_in_queue} URLs...")
    print(f"⚡ Speed: 0.3s delay between requests\n")
    
    # Crawl ALL of them
    urls = [item['url'] for item in queue_items]
    pages = crawler.crawl(urls)
    
    if pages:
        print(f"\n📊 Successfully crawled {len(pages)}/{total_in_queue} pages")
        print(f"📊 Building index...")
        
        index = indexer.build_index(pages)
        
        if index:
            print(f"💾 Saving to Back4App ({index['doc_count']} docs, {len(index['index'])} unique terms)...")
            
            if indexer.save_to_back4app(index):
                # Delete ALL queue items that were processed
                queue_ids = [item['objectId'] for item in queue_items]
                delete_queue_items(queue_ids)
                print(f"\n✅ Done! Indexed {len(pages)} pages. Queue cleared.")
            else:
                print("❌ Failed to save index")
        else:
            print("❌ Failed to build index")
    else:
        print("❌ No pages were successfully crawled")


if __name__ == "__main__":
    start = time.time()
    main()
    elapsed = time.time() - start
    print(f"\n⏱ Total time: {elapsed:.1f} seconds")
