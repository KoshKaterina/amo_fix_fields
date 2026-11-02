"""Representative context must reach both read and write requests."""
import asyncio
import unittest
from unittest.mock import patch
import httpx
import metrika_client

class RepresentativeTests(unittest.IsolatedAsyncioTestCase):
    async def request_case(self, login):
        seen = []
        def handle(request):
            seen.append(request)
            if request.method == 'GET':
                return httpx.Response(200, json={'counters': [{'id': 93845803}]})
            return httpx.Response(202, json={'uploading': {'id': 1}})
        client = metrika_client.MetrikaClient('https://api-metrika.yandex.net', 'test-secret', login)
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            self.assertEqual(await client.get_counters(), [{'id': 93845803}])
            result = await client.upload_simple_orders(93845803, [{'id': 123, 'create_date_time': '2026-11-02 10:00:00', 'client_ids': '456'}])
            self.assertEqual(result['uploading']['id'], 1)
        finally:
            await client.aclose()
        self.assertEqual(len(seen), 2)
        for r in seen:
            self.assertEqual(r.url.params.get('ulogin'), login.strip() or None)
            self.assertEqual(r.headers['Authorization'], 'OAuth test-secret')
            self.assertNotIn('test-secret', str(r.url))
        self.assertEqual(seen[1].url.params['merge_mode'], 'UPDATE')
        self.assertEqual(seen[1].url.params['delimiter_type'], 'COMMA')
        self.assertIn(b'client_ids', seen[1].content)
    async def test_representative(self):
        await self.request_case('main@sunscrypt.ru')
    async def test_direct_access_unchanged(self):
        await self.request_case('')
    async def test_whitespace_trimmed(self):
        await self.request_case(' main@sunscrypt.ru ')
    async def test_auth_failure_not_retried(self):
        seen = []
        def handle(request):
            seen.append(request)
            return httpx.Response(403, json={'message': 'Invalid oauth_token'})
        client = metrika_client.MetrikaClient('https://api-metrika.yandex.net', 'test-secret', 'main@sunscrypt.ru')
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        try:
            with self.assertRaises(metrika_client.MetrikaError) as ctx:
                await client.upload_simple_orders(93845803, [{'id': 123}])
            self.assertEqual(ctx.exception.status, 403)
            self.assertEqual(len(seen), 1)
        finally:
            await client.aclose()
    async def test_module_init_passes_context(self):
        with patch.object(metrika_client, 'METRIKA_ULOGIN', 'main@sunscrypt.ru'):
            await metrika_client.init()
            try:
                self.assertEqual(metrika_client._client._ulogin, 'main@sunscrypt.ru')
            finally:
                await metrika_client.aclose()

if __name__ == '__main__':
    unittest.main()
