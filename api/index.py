from http.server import BaseHTTPRequestHandler
import json
import requests
import math
from urllib.parse import parse_qs, urlparse

# ===== CONFIG =====
PARSE_APP_ID = "YOUR_APP_ID"
PARSE_REST_KEY = "YOUR_REST_KEY"
PARSE_URL = "https://parseapi.back4app.com"

HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
}
# ==================

class SearchEngine:
    def __init__(self):
        self.index_cache = None
        self.last_load = 0
    
    def load_index(self):
        """Load index from Back4App, cache for 60 seconds"""
        if self.index_cache and (__import__('time').time() - self.last_load < 60):
            return self.index_cache
        
        resp = requests.get(f"{PARSE_URL}/classes/Index/main", headers=HEADERS)
        if resp.status_code == 200:
            data = resp.json()
            self.index_cache = json.loads(data['data'])
            self.last_load = __import__('time').time()
            return self.index_cache
        return None
    
    def search(self, query, max_results=20):
        """Search the index and return ranked results"""
        index = self.load_index()
        if not index:
            return []
        
        # Tokenize query
        words = query.lower().split()
        
        # Score documents
        scores = {}
        for word in words:
            if word in index['index']:
                for doc_id, tfidf in index['index'][word].items():
                    scores[doc_id] = scores.get(doc_id, 0) + tfidf
        
        # Rank results
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:max_results]
        
        results = []
        for doc_id, score in ranked:
            idx = int(doc_id)
            results.append({
                "url": index['urls'][idx],
                "title": index['titles'][idx],
                "snippet": index['snippets'][idx][:200],
                "score": round(score, 4)
            })
        
        return results


search_engine = SearchEngine()

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        
        if path in ['/api/index', '/search']:
            query = parse_qs(urlparse(self.path).query).get('q', [''])[0]
            results = search_engine.search(query)
            
            self._respond(200, {"results": results, "query": query})
        else:
            self._respond(404, {"error": "Not found"})
    
    def do_POST(self):
        path = urlparse(self.path).path
        
        if path in ['/api/index', '/upload']:
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length)) if content_length else {}
            url = body.get('url', '')
            
            # Validate URL
            if not url or not urlparse(url).scheme:
                self._respond(400, {"error": "Valid URL required"})
                return
            
            # Add to queue (store in Back4App Queue class)
            queue_item = {"url": url, "status": "pending"}
            resp = requests.post(
                f"{PARSE_URL}/classes/CrawlQueue",
                json=queue_item,
                headers=HEADERS
            )
            
            self._respond(200, {
                "success": True,
                "message": f"URL queued: {url}",
                "crawlNext": "Run crawler.py to add it to the index"
            })
        else:
            self._respond(404, {"error": "Not found"})
    
    def _respond(self, status, data):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
