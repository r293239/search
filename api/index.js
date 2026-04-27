const https = require('https');

const PARSE_APP_ID = "qXJqQ3HWKYsGVB1oQKnYZo7zdNLHgjZMiwonhozr";
const PARSE_REST_KEY = "mdTfymJLDHJY46HUv0tgKtWkqMm4YHQEbdsPX8tJ";
const PARSE_HOST = "parseapi.back4app.com";

const HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
};

class SearchEngine {
    constructor() {
        this.indexCache = null;
        this.lastLoad = 0;
    }

    async loadIndex() {
        if (this.indexCache && (Date.now() - this.lastLoad < 60000)) {
            return this.indexCache;
        }

        return new Promise((resolve) => {
            const options = {
                hostname: PARSE_HOST,
                path: '/classes/Index?order=-createdAt&limit=1',
                method: 'GET',
                headers: HEADERS
            };
            const req = https.request(options, (res) => {
                let data = '';
                res.on('data', chunk => data += chunk);
                res.on('end', () => {
                    try {
                        const json = JSON.parse(data);
                        const results = json.results || [];
                        if (results.length > 0 && results[0].data) {
                            this.indexCache = JSON.parse(results[0].data);
                            this.lastLoad = Date.now();
                            resolve(this.indexCache);
                        } else {
                            resolve(null);
                        }
                    } catch (e) {
                        resolve(null);
                    }
                });
            });
            req.on('error', () => resolve(null));
            req.end();
        });
    }

    async search(query, maxResults = 20) {
        const index = await this.loadIndex();
        if (!index || !index.index) return [];

        const words = query.toLowerCase().split(/\s+/).filter(w => w.length > 0);
        const scores = {};

        for (const word of words) {
            if (index.index[word]) {
                for (const [docId, tfidf] of Object.entries(index.index[word])) {
                    scores[docId] = (scores[docId] || 0) + tfidf;
                }
            }
        }

        const ranked = Object.entries(scores)
            .sort((a, b) => b[1] - a[1])
            .slice(0, maxResults);

        return ranked.map(([docId, score]) => {
            const id = parseInt(docId);
            return {
                url: index.urls[id] || '',
                title: index.titles[id] || 'Untitled',
                snippet: (index.snippets[id] || '').substring(0, 200),
                score: Math.round(score * 10000) / 10000
            };
        });
    }
}

const searchEngine = new SearchEngine();

function makeRequest(options, body = null) {
    return new Promise((resolve) => {
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
                catch(e) { resolve({ status: res.statusCode, data }); }
            });
        });
        req.on('error', (e) => resolve({ status: 500, data: { error: e.message } }));
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

function getQueryParams(url) {
    const queryString = (url || '').split('?')[1] || '';
    const params = {};
    queryString.split('&').forEach(pair => {
        const [key, val] = pair.split('=');
        if (key) params[decodeURIComponent(key)] = decodeURIComponent(val || '');
    });
    return params;
}

module.exports = async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.status(200).end();
        return;
    }

    const path = (req.url || '/').split('?')[0];

    if (req.method === 'GET') {
        const params = getQueryParams(req.url);
        const query = params.q || '';
        const results = query ? await searchEngine.search(query) : [];
        res.status(200).json({ results, query });
    }
    else if (req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            try {
                const { url } = JSON.parse(body || '{}');
                if (!url || !url.startsWith('http')) {
                    res.status(400).json({ error: 'Valid URL required' });
                    return;
                }

                const options = {
                    hostname: PARSE_HOST,
                    path: '/classes/CrawlQueue',
                    method: 'POST',
                    headers: HEADERS
                };

                const result = await makeRequest(options, { url, status: 'pending' });

                if (result.status === 200 || result.status === 201) {
                    res.status(200).json({
                        success: true,
                        message: `URL queued: ${url}`,
                        crawlNext: 'GitHub Actions will crawl it automatically!'
                    });
                } else {
                    res.status(500).json({ error: 'Failed to queue URL' });
                }
            } catch (e) {
                res.status(400).json({ error: 'Invalid request body' });
            }
        });
    }
    else {
        res.status(404).json({ error: 'Not found' });
    }
};
