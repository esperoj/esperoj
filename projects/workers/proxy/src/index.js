export default {
    async fetch(request) {
        const RANGE_RETRY_ATTEMPTS = 2;
        const url = new URL(request.url);
        const path = url.href.substring(url.origin.length + 1);
        const ttl = parseInt(request.headers.get('esperoj-cache-ttl'), 10) || 365 * 24 * 3600;
        const isRangeRequest = request.headers.has('Range');
        const targetUrl = (!isRangeRequest && (request.headers.get('esperoj-use-0ms') == "1" || !request.headers.has('esperoj-use-0ms')) && request.method == 'GET')
        ? "https://x.0ms.dev/q70/" + path: path;

        async function fetchWithRedirect(targetUrl) {
            const headers = new Headers(request.headers);
            const modifiedRequest = new Request(targetUrl, {
                method: request.method,
                headers: headers,
                body: request.method !== 'GET' && request.method !== 'HEAD' ? request.body: null,
                redirect: 'manual',
                cf: {
                    cacheTtlByStatus: {
                        "200-299": isRangeRequest ? -1: ttl,
                        "404": -1,
                        "500-599": -1
                    }
                }
            });
            let response = await fetch(modifiedRequest);
            if (response.status >= 300 && response.status < 400) {
                const location = response.headers.get('Location');
                if (location) {
                    const redirectUrl = new URL(location, targetUrl);
                    return fetchWithRedirect(redirectUrl);
                }
            }
            if (response.status >= 200 && response.status <= 299 && request.method == 'GET') {
                const newResponse = new Response(response.body, response);
                newResponse.headers.set('Cache-Control', (isRangeRequest || ttl < 1) ? 'no-store': `public, max-age=${ttl}, immutable`);
                return newResponse;
            }
            return response;
        }
        return fetchWithRedirect(targetUrl);
    },
};