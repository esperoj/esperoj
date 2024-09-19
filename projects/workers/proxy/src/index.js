export default {
    async fetch(request) {
        const url = new URL(request.url);
        const path = url.href.substring(url.origin.length + 1);
        if (!path.startsWith('http')) {
            return new Response('Invalid URL', {
                status: 400
            });
        }
        async function handleRequest(targetUrl) {
            const headers = new Headers(request.headers);
            const modifiedRequest = new Request(targetUrl, {
                method: request.method,
                headers: headers,
                body: request.method !== 'GET' && request.method !== 'HEAD' ? request.body: null,
                redirect: 'manual',
            });

            const response = await fetch(modifiedRequest);
            if (response.status >= 300 && response.status < 400) {
                const location = response.headers.get('Location');
                if (location) {
                    const redirectUrl = new URL(location, targetUrl);
                    return handleRequest(redirectUrl);
                }
            }
            return response;
        }
        return handleRequest(path);
    },
};