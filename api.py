import asyncio
import os
import logging
from pprint import pprint

from dotenv import load_dotenv
import httpx

load_dotenv()

logger = logging.getLogger("uvicorn")

integration_id = os.environ.get('INTEGRATION_ID')
secret_key = os.getenv('SECRET_KEY')


current_token = os.getenv('TOKEN')

headers = {
    f'Authorization': f'Bearer {current_token}'
 }


async def auth():
    body = {
        'client_id': integration_id,
        'client_secret': secret_key,
        'grant_type': 'authorization_code',
        'code': 'def5020093463e984c956d5b3258cfad73c1387473a85cd733b384576db0555198b3f674efacec89407f4a055d619eee71b693c80a3ae045e05418a0ef2a098ebbf43f9f0405c56ac3c419bd9e3479d0f6fca16146fdf7b0ca844a3563bed928d79dfcfb2e0445314bea6d470b5c36aaeb146bb58647078e7829cb190ef600f1072dd36ecd7230cd7e6ae4830bf0e251d5321f7f5d564d77f2cd597e2508423fb391f05760d10f4c88d1d4ba783c62852b489510dba58e0e2540ba54e93afcafda77a7b0a29d1b35c20d1c6da55fcb4733224d1b0e66e2f2caea774071d6efd717403e17906a0e48af31ca1e5e3a50246a64070cdea3b48417b719a060b8cc4a44cd6736cf82d207c4c1288c3d3ecf20b93e413fa138d243b542c6db85e154aa606a0a3066b675e6e0d882832e7ccbfbceea0e6d417438f08bfbdd79b198f144c59127b62164395a1bf152ed19415a6a3cb7bf0a354e7e84e16bbe7549cf0bbf3815403bcbb7ee56ac16f1efff5318ae529758ca8c15b91ccdef1f636f46f532aff17bc2573005a4a7997846b36ffb6988badaefa6b5c46606d6efc80d2ce796a5f6ade82be70998ecc9c082cd5af915cffaefa422db2ba94edda6cc5d98f6066a2dd446980c9449fc5c75299a60a889737aa15f7924fffd2fc1fe3846bdae55ceede77e0b8fdba81f5fa3ae6c9031d76a26ac',
        'redirect_uri': 'https://new5a2e8ea7b16b4.amocrm.ru'
    }
    async with httpx.AsyncClient() as client:
        response = (await client.post('https://new5a2e8ea7b16b4.amocrm.ru/oauth2/access_token', data=body)).json()


async def get_lead_by_id(lead_id):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f'https://new5a2e8ea7b16b4.amocrm.ru/api/v4/leads/{lead_id}', headers=headers)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"AmoCRM error {response.status_code} for lead {lead_id}: {response.text}")
                return None
        except Exception as e:
            logger.error(f"Request error fetching lead {lead_id}: {e}")
            return None

async def add_info_from_ms(goods, delivery_type, delivery_address, lead_id, name):

    custom_fields = []
    if goods:
        custom_fields.append(await create_custom_field(goods, 577313))
    if delivery_type:
        custom_fields.append(await create_custom_field(delivery_type, 577315))
    if delivery_address:
        custom_fields.append(await create_custom_field(delivery_address, 577311))




    body = {
        'id': lead_id,
        'custom_fields_values': custom_fields,
    }
    if name:
        body['name'] = name

    async with httpx.AsyncClient() as client:
        try:
            response = await client.patch(f'https://new5a2e8ea7b16b4.amocrm.ru/api/v4/leads/{lead_id}', headers=headers, json=body)
            if response.status_code in [200, 204]:
                logger.info(f"Successfully updated lead {lead_id}")
                return True
            else:
                logger.error(f"Failed to update lead {lead_id}: {response.status_code} {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error patching lead {lead_id}: {e}")
            return False


async def create_custom_field(value, id):
    new_field = {
            'field_id': id,
            'values': [
                {
                    'value': value
                }
            ]
        }
    return new_field

if __name__ == '__main__':
    asyncio.run(get_lead_by_id(36349059))
