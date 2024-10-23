import { env, createExecutionContext, waitOnExecutionContext, SELF } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import worker from '../src';

describe('Proxy worker', () => {
	it('responds with echo (unit style)', async () => {
		const request = new Request("https://example.com/https://echo.free.beeceptor.com");
        const ctx = createExecutionContext();
		const response = await worker.fetch(request, env, ctx);
        await waitOnExecutionContext(ctx);
		expect((await response.json())["method"]).toBe("GET");
	});

});
