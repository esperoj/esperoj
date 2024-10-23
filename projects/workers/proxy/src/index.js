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
                if (isRangeRequest) {
                    let attempts = RANGE_RETRY_ATTEMPTS;
                    do {
                        let controller = new AbortController();
                        response = await fetch(modifiedRequest, {
                            signal: controller.signal
                        });
                        if (response.headers.has("content-range")) {
                            if (attempts < RANGE_RETRY_ATTEMPTS) {
                                console.log(`Retry for ${targetUrl} succeeded - response has content-range header`);
                            }
                            break;
                        } else if (response.ok) {
                            attempts -= 1;
                            console.error(`Range header in request for ${targetUrl} but no content-range header in response. Will retry ${attempts} more times`);
                            if (attempts > 0) {
                                controller.abort();
                            }
                        } else {
                            break;
                        }
                    } while (attempts > 0);
                    if (attempts <= 0) {
                        console.error(`Tried range request for ${targetUrl} ${RANGE_RETRY_ATTEMPTS} times, but no content-range in response.`);
                    }
                }
                const newResponse = new Response(response.body, response);
                newResponse.headers.set('Cache-Control', isRangeRequest ? 'no-cache': `public, max-age=${ttl}, immutable`);
                return newResponse;
            }
            return response;
        }
        return fetchWithRedirect(targetUrl);
    },
};