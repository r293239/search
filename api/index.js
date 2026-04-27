const https = require('https');
const crypto = require('crypto');

const PARSE_APP_ID = "qXJqQ3HWKYsGVB1oQKnYZo7zdNLHgjZMiwonhozr";
const PARSE_REST_KEY = "mdTfymJLDHJY46HUv0tgKtWkqMm4YHQEbdsPX8tJ";
const PARSE_HOST = "parseapi.back4app.com";

const HEADERS = {
    "X-Parse-Application-Id": PARSE_APP_ID,
    "X-Parse-REST-API-Key": PARSE_REST_KEY,
    "Content-Type": "application/json"
};

function hashPassword(password) {
    return crypto.createHash('sha256').update(password + 'search-engine-salt').digest('hex');
}

function parseRequest(method, path, body = null) {
    return new Promise((resolve) => {
        const options = {
            hostname: PARSE_HOST,
            path: path,
            method: method,
            headers: HEADERS
        };
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try { resolve(JSON.parse(data)); }
                catch(e) { resolve({ error: 'Parse error', raw: data }); }
            });
        });
        req.on('error', (e) => resolve({ error: e.message }));
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

class SearchEngine {
    constructor() {
        this.indexCache = null;
        this.lastLoad = 0;
    }

    async loadIndex() {
        if (this.indexCache && (Date.now() - this.lastLoad < 60000)) {
            return this.indexCache;
        }
        const data = await parseRequest('GET', '/classes/Index?order=-createdAt&limit=1');
        const results = data.results || [];
        if (results.length > 0 && results[0].data) {
            this.indexCache = JSON.parse(results[0].data);
            this.lastLoad = Date.now();
            return this.indexCache;
        }
        return null;
    }

    async search(query, maxResults = 50) {
        const index = await this.loadIndex();
        if (!index || !index.index) return { results: [], totalIndexed: 0 };
        const words = query.toLowerCase().split(/\s+/).filter(w => w.length > 0);
        const scores = {};
        for (const word of words) {
            if (index.index[word]) {
                for (const [docId, tfidf] of Object.entries(index.index[word])) {
                    scores[docId] = (scores[docId] || 0) + tfidf;
                }
            }
        }
        const ranked = Object.entries(scores).sort((a, b) => b[1] - a[1]).slice(0, maxResults);
        const results = ranked.map(([docId, score]) => {
            const id = parseInt(docId);
            return {
                url: index.urls[id] || '',
                title: index.titles[id] || 'Untitled',
                snippet: (index.snippets[id] || '').substring(0, 250),
                score: Math.round(score * 10000) / 10000
            };
        });
        return { results, totalIndexed: index.doc_count || index.urls.length || 0 };
    }
}

const searchEngine = new SearchEngine();

// ===== AUTH HELPERS =====
async function getUser(username) {
    const where = encodeURIComponent(JSON.stringify({ username }));
    const data = await parseRequest('GET', `/classes/Users?where=${where}&limit=1`);
    return (data.results && data.results.length > 0) ? data.results[0] : null;
}

async function createUser(username, password, role) {
    return parseRequest('POST', '/classes/Users', {
        username,
        password: hashPassword(password),
        role: role || 'viewer'
    });
}

async function getAllUsers() {
    const data = await parseRequest('GET', '/classes/Users?order=createdAt');
    return data.results || [];
}

async function updateUserRole(objectId, newRole) {
    return parseRequest('PUT', `/classes/Users/${objectId}`, { role: newRole });
}

async function deleteUserById(objectId) {
    return parseRequest('DELETE', `/classes/Users/${objectId}`);
}

async function getUserData(userId) {
    const where = encodeURIComponent(JSON.stringify({ userId }));
    const data = await parseRequest('GET', `/classes/UserData?where=${where}&limit=1`);
    return (data.results && data.results.length > 0) ? data.results[0] : null;
}

async function saveUserData(userId, history, saved) {
    const existing = await getUserData(userId);
    if (existing) {
        return parseRequest('PUT', `/classes/UserData/${existing.objectId}`, {
            userId, history, saved
        });
    } else {
        return parseRequest('POST', '/classes/UserData', {
            userId, history, saved
        });
    }
}

// ===== QUEUE HELPERS =====
async function getQueue() {
    const where = encodeURIComponent(JSON.stringify({ status: 'pending' }));
    const data = await parseRequest('GET', `/classes/CrawlQueue?where=${where}&order=-createdAt&limit=100`);
    return data.results || [];
}

async function bulkAddToQueue(urls) {
    let count = 0;
    let skipped = 0;
    const pending = await getQueue();
    const existingUrls = new Set(pending.map(p => p.url));
    
    for (const url of urls) {
        if (existingUrls.has(url)) {
            skipped++;
            continue;
        }
        const result = await parseRequest('POST', '/classes/CrawlQueue', { url, status: 'pending' });
        if (result.objectId) {
            count++;
            existingUrls.add(url);
        }
    }
    return { count, skipped };
}

async function deleteQueueItem(objectId) {
    return parseRequest('DELETE', `/classes/CrawlQueue/${objectId}`);
}

async function clearQueue() {
    const pending = await getQueue();
    for (const item of pending) {
        await deleteQueueItem(item.objectId);
    }
    return { cleared: pending.length };
}

async function getStats() {
    const index = await searchEngine.loadIndex();
    const pending = await getQueue();
    const users = await getAllUsers();
    return {
        indexed: index ? (index.doc_count || index.urls.length || 0) : 0,
        terms: index ? Object.keys(index.index || {}).length : 0,
        queue: pending.length,
        users: users.length
    };
}

async function getIndexPages() {
    const index = await searchEngine.loadIndex();
    if (!index) return [];
    return (index.urls || []).map((url, i) => ({
        url,
        title: index.titles[i] || 'Untitled',
        snippet: index.snippets[i] || ''
    }));
}

// ===== MAIN HANDLER =====
module.exports = async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.status(200).end();
        return;
    }

    // GET - Search
    if (req.method === 'GET') {
        const url = req.url || '';
        const queryString = url.split('?')[1] || '';
        const params = {};
        queryString.split('&').forEach(pair => {
            const [key, val] = pair.split('=');
            if (key) params[decodeURIComponent(key)] = decodeURIComponent(val || '');
        });
        
        const query = params.q || '';
        const limit = parseInt(params.limit) || 50;
        
        if (query) {
            const data = await searchEngine.search(query, limit);
            res.status(200).json(data);
        } else {
            const index = await searchEngine.loadIndex();
            res.status(200).json({
                results: [],
                query: '',
                totalIndexed: index ? (index.doc_count || index.urls.length || 0) : 0
            });
        }
        return;
    }

    // POST - Auth, Admin, User data
    if (req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', async () => {
            let data;
            try { data = JSON.parse(body || '{}'); } catch(e) { data = {}; }
            
            const action = data.action || '';

            // ===== AUTH =====
            if (action === 'login') {
                const { username, password } = data;
                if (!username || !password) {
                    res.status(400).json({ error: 'Username and password required' });
                    return;
                }

                let user = await getUser(username);
                
                if (!user) {
                    const allUsers = await getAllUsers();
                    const role = allUsers.length === 0 ? 'admin' : 'viewer';
                    const created = await createUser(username, password, role);
                    res.status(200).json({
                        success: true,
                        user: {
                            username,
                            role,
                            objectId: created.objectId
                        }
                    });
                    return;
                }

                if (user.password !== hashPassword(password)) {
                    res.status(401).json({ error: 'Invalid password' });
                    return;
                }

                res.status(200).json({
                    success: true,
                    user: {
                        username: user.username,
                        role: user.role,
                        objectId: user.objectId
                    }
                });
                return;
            }

            // ===== USER DATA =====
            if (action === 'saveUser') {
                const { user } = data;
                if (!user || !user.email) {
                    res.status(400).json({ error: 'Invalid user' });
                    return;
                }
                // Just ensure user exists in Users table
                let existing = await getUser(user.email);
                if (!existing) {
                    await createUser(user.email, 'oauth-' + Date.now(), 'viewer');
                }
                res.status(200).json({ success: true });
                return;
            }

            if (action === 'getUserData') {
                const { userId } = data;
                const userData = await getUserData(userId);
                res.status(200).json({
                    history: userData ? userData.history : [],
                    saved: userData ? userData.saved : []
                });
                return;
            }

            if (action === 'saveHistory') {
                const { userId, history } = data;
                const existing = await getUserData(userId);
                const saved = existing ? existing.saved : [];
                await saveUserData(userId, history, saved);
                res.status(200).json({ success: true });
                return;
            }

            if (action === 'savePages') {
                const { userId, saved } = data;
                const existing = await getUserData(userId);
                const history = existing ? existing.history : [];
                await saveUserData(userId, history, saved);
                res.status(200).json({ success: true });
                return;
            }

            // ===== ADMIN =====
            if (action === 'bulkUpload') {
                const { urls, username, role } = data;
                if (!username || !role || role === 'viewer') {
                    res.status(403).json({ error: 'Permission denied' });
                    return;
                }
                if (!urls || !Array.isArray(urls) || urls.length === 0) {
                    res.status(400).json({ error: 'No URLs provided' });
                    return;
                }
                const result = await bulkAddToQueue(urls);
                res.status(200).json({ success: true, ...result });
                return;
            }

            if (action === 'getQueue') {
                const queue = await getQueue();
                res.status(200).json({ queue });
                return;
            }

            if (action === 'deleteQueueItem') {
                await deleteQueueItem(data.objectId);
                res.status(200).json({ success: true });
                return;
            }

            if (action === 'clearQueue') {
                const result = await clearQueue();
                res.status(200).json({ success: true, ...result });
                return;
            }

            if (action === 'getUsers') {
                const user = await getUser(data.username);
                if (!user || user.role !== 'admin') {
                    res.status(403).json({ error: 'Admin only' });
                    return;
                }
                const users = await getAllUsers();
                res.status(200).json({ users });
                return;
            }

            if (action === 'changeRole') {
                const user = await getUser(data.username);
                if (!user || user.role !== 'admin') {
                    res.status(403).json({ error: 'Admin only' });
                    return;
                }
                await updateUserRole(data.targetId, data.newRole);
                res.status(200).json({ success: true });
                return;
            }

            if (action === 'deleteUser') {
                const user = await getUser(data.username);
                if (!user || user.role !== 'admin') {
                    res.status(403).json({ error: 'Admin only' });
                    return;
                }
                await deleteUserById(data.targetId);
                res.status(200).json({ success: true });
                return;
            }

            if (action === 'getStats') {
                const stats = await getStats();
                res.status(200).json(stats);
                return;
            }

            if (action === 'getIndexPages') {
                const pages = await getIndexPages();
                res.status(200).json({ pages });
                return;
            }

            // Default: single URL upload
            const url = data.url;
            if (url && url.startsWith('http')) {
                const result = await parseRequest('POST', '/classes/CrawlQueue', { url, status: 'pending' });
                res.status(200).json({
                    success: true,
                    message: `URL queued: ${url}`,
                    crawlNext: 'The auto-discover crawler will index it soon!'
                });
                return;
            }

            res.status(400).json({ error: 'Unknown action' });
        });
        return;
    }

    res.status(404).json({ error: 'Not found' });
};
