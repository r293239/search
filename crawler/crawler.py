import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import re
import math
import time
import json
from collections import defaultdict

# ===== CONFIG (replace with your Back4App keys) =====
PARSE_APP_ID = "YOUR_APP_ID"
PARSE_REST_KEY = "YOUR_REST_KEY"
PARSE_URL = "https://parseapi.back4app.com"

HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
}
# ===================================================

class Crawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SearchBot/1.0)"})
    
    def fetch(self, url):
        """Download and parse a webpage"""
        resp = self.session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        title = soup.title.string.strip() if soup.title else url
        text = soup.get_text(separator=' ', strip=True)[:10000]
        links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True) 
                 if urljoin(url, a['href']).startswith('http')][:20]
        return {"url": url, "title": title, "text": text, "links": links}
    
    def crawl(self, start_urls, max_pages=50):
        """BFS crawl from seed URLs"""
        visited = set()
        queue = list(start_urls)
        pages = []
        
        while queue and len(pages) < max_pages:
            url = queue.pop(0)
            if url in visited: continue
            visited.add(url)
            try:
                page = self.fetch(url)
                pages.append(page)
                queue.extend(page['links'])
                print(f"✓ [{len(pages)}/{max_pages}] {url}")
                time.sleep(1)  # Be polite
            except Exception as e:
                print(f"✗ {url}: {e}")
        return pages


class Indexer:
    def __init__(self):
        self.stopwords = {"the","a","an","is","are","was","were","be","been","being",
                         "have","has","had","do","does","did","will","would","shall",
                         "should","may","might","must","can","could","of","in","to",
                         "for","with","on","at","by","from","and","or","not","but",
                         "if","then","else","when","where","why","how","all","any",
                         "both","each","few","more","most","other","some","such","no",
                         "only","own","same","so","than","too","very","this","that",
                         "it","its","he","she","they","them","these","those","i","my",
                         "your","we","our","you","me","us","him","his","her","their"}
    
    def tokenize(self, text):
        """Split text into lowercase words, remove stopwords/short words"""
        words = re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())
        return [w for w in words if w not in self.stopwords]
    
    def build_index(self, pages):
        """Build inverted index with TF-IDF scores"""
        # Count documents
        N = len(pages)
        
        # Tokenize all pages
        doc_tokens = {}
        doc_urls = []
        for i, page in enumerate(pages):
            tokens = self.tokenize(page['text'])
            doc_tokens[i] = tokens
            doc_urls.append(page['url'])
        
        # Compute document frequencies
        df = defaultdict(int)  # word -> how many docs contain it
        for tokens in doc_tokens.values():
            unique_words = set(tokens)
            for word in unique_words:
                df[word] += 1
        
        # Build inverted index with TF-IDF weights
        index = {}  # word -> {doc_id: tfidf_score}
        for i, tokens in doc_tokens.items():
            # Term frequencies in this doc
            tf = defaultdict(int)
            for w in tokens:
                tf[w] += 1
            max_tf = max(tf.values()) if tf else 1
            
            for word, count in tf.items():
                if word not in index:
                    index[word] = {}
                # TF-IDF formula
                tf_norm = count / max_tf
                idf = math.log((N - df[word] + 0.5) / (df[word] + 0.5) + 1)
                index[word][str(i)] = round(tf_norm * idf, 4)
        
        return {
            "index": index,
            "urls": doc_urls,
            "titles": [p['title'] for p in pages],
            "snippets": [p['text'][:300] for p in pages],
            "doc_count": N,
            "timestamp": time.time()
        }
    
    def save_to_back4app(self, index_data, index_name="main"):
        """Store index in Back4App Index class"""
        # Delete old index
        requests.delete(f"{PARSE_URL}/classes/Index/{index_name}", headers=HEADERS)
        
        # Save new index
        payload = {
            "objectId": index_name,
            "data": json.dumps(index_data),
            "docCount": index_data["doc_count"],
            "timestamp": int(index_data["timestamp"])
        }
        resp = requests.put(f"{PARSE_URL}/classes/Index/{index_name}", json=payload, headers=HEADERS)
        print(f"Index saved: {resp.status_code} - {resp.json()}")
        return resp.ok


def main():
    crawler = Crawler()
    indexer = Indexer()
    
    # Seed URLs to build initial index
    seeds = [
        "https://en.wikipedia.org/wiki/Web_crawler",
        "https://en.wikipedia.org/wiki/Search_engine_indexing",
        "https://developer.mozilla.org/en-US/docs/Web/HTTP"
    ]
    
    print("🕷️ Crawling...")
    pages = crawler.crawl(seeds, max_pages=20)
    
    if pages:
        print(f"\n📊 Building index from {len(pages)} pages...")
        index = indexer.build_index(pages)
        
        print(f"💾 Saving to Back4App...")
        indexer.save_to_back4app(index)
        print(f"✅ Done! Index ready with {len(index['index'])} unique terms.")
    else:
        print("❌ No pages crawled.")


if __name__ == "__main__":
    main()
