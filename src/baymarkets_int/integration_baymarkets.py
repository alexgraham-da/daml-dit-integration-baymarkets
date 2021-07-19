import asyncio
import base64
from dataclasses import dataclass
import hashlib
import hmac
import logging
import time

from aiohttp import ClientSession, BasicAuth
from dazl import create, exercise

from daml_dit_if.api import \
    IntegrationEnvironment, IntegrationEvents

LOG = logging.getLogger('dabl-integration-baymarkets')

BAYMARKETS_FEED = 'admin/dabl/dabl-feed' # POST / DELETE
BAYMARKETS_SYSTEM_STATUS = 'system-status' # GET
BAYMARKETS_CLEARED_POSITIONS = 'cleared-positions' # GET
BAYMARKETS_COLLATERAL_POSITIONS = 'collateral-positions' # GET
BAYMARKETS_MARGIN_CALC = 'operations/margin-calculation' # POST
BAYMARKETS_MARK_TO_MARKET_CALC = 'operations/mark-to-market-calculation' # POST


class BAYMARKETS:
    ErrorResponse = 'Baymarkets.Integration:ErrorResponse'
    RequestSystemStatus = 'Baymarkets.Integration:RequestSystemStatus'
    ClaraSystemStatus = 'Baymarkets.Integration:ClaraSystemStatus'
    RequestClearedPositions = 'Baymarkets.Integration:RequestClearedPositions'
    ClearedPosition = 'Baymarkets.Integration:ClearedPosition'
    RequestCollateralPositions = 'Baymarkets.Integration:RequestCollateralPositions'
    CollateralPosition = 'Baymarkets.Integration:CollateralPosition'
    RequestMarginCalculation = 'Baymarkets.Integration:RequestMarginCalculation'
    MarginCalculationResponse = 'Baymarkets.Integration:MarginCalculationResponse'
    RequestMarkToMarketCalculation = 'Baymarkets.Integration:RequestMarkToMarketCalculation'
    MarkToMarketCalculationResponse = 'Baymarkets.Integration:MarkToMarketCalculationResponse'


@dataclass
class BaymarketsIntegrationEnv(IntegrationEnvironment):
    username: str
    password: str
    serverUrl: str


def integration_baymarkets_main(
    env: 'BaymarketsIntegrationEnv',
    events: 'IntegrationEvents'):

    @events.ledger.contract_created(BAYMARKETS.RequestSystemStatus)
    async def handle_request_system_status(event):
        commands = [exercise(event.cid, 'Archive', {})]
        async with ClientSession() as session:
            data_dict = {}
            LOG.info(f'Integration ==> Baymarkets: POST {data_dict}')
            async with session.get(url = f'{env.serverUrl}/{BAYMARKETS_SYSTEM_STATUS}',
                                   json=data_dict,
                                   auth = BasicAuth(env.username, env.password)) as resp:
                json_resp = await resp.json()
                LOG.info(f'Integration <== Exberry: {json_resp}')
                commands.append(create(BAYMARKETS.ClaraSystemStatus, {
                    'integrationParty': env.party,
                    'environment': json_resp['environment'],
                    'version': json_resp['version'],
                    'scmRevision': json_resp['scmRevision']
                }))
        return commands

    @events.ledger.contract_created(BAYMARKETS.RequestClearedPositions)
    async def handle_request_cleared_positions(event):
        commands = [exercise(event.cid, 'Archive', {})]
        async with ClientSession() as session:
            data_dict = {}
            LOG.info(f'Integration ==> Baymarkets: GET {data_dict}')
            async with session.get(url = f'{env.serverUrl}/{BAYMARKETS_CLEARED_POSITIONS}',
                                   json=data_dict,
                                   auth = BasicAuth(env.username, env.password)) as resp:
                json_resp = await resp.json()
                LOG.info(f'Integration <== Exberry: {json_resp}')
                for position in json_resp:
                    commands.append(create(BAYMARKETS.ClearedPosition, {
                        'integrationParty': env.party,
                        'accountId': position['accountId'],
                        'instrumentId': position['instrumentId'],
                        'currencyId': position['currencyId'],
                        'quantityCredit': position['quantityCredit'],
                        'quantityDebit': position['quantityDebit'],
                        'amountCredit': position['amountCredit'],
                        'amountDebit': position['amountDebit']
                    }))
        return commands

    @events.ledger.contract_created(BAYMARKETS.RequestCollateralPositions)
    async def handle_request_collateral_positions(event):
        commands = [exercise(event.cid, 'Archive', {})]
        async with ClientSession() as session:
            data_dict = {}
            LOG.info(f'Integration ==> Baymarkets: GET {data_dict}')
            async with session.get(url = f'{env.serverUrl}/{BAYMARKETS_COLLATERAL_POSITIONS}',
                                   json=data_dict,
                                   auth = BasicAuth(env.username, env.password)) as resp:
                json_resp = await resp.json()
                LOG.info(f'Integration <== Exberry: {json_resp}')
                if 'error' in json_resp:
                    commands.append(create_error_response(json_resp))
                else:
                    for position in json_resp:
                        commands.append(create(BAYMARKETS.CollateralPosition, {
                            'integrationParty': env.party,
                            'accountId': position['accountId'],
                            'assetId': position['assetId'],
                            'quantity': position['quantity']
                        }))
        return commands

    @events.ledger.contract_created(BAYMARKETS.RequestMarginCalculation)
    async def handle_request_margin_calculation(event):
        commands = [exercise(event.cid, 'Archive', {})]
        async with ClientSession() as session:
            data_dict = {}
            LOG.info(f'Integration ==> Baymarkets: GET {data_dict}')
            async with session.post(url = f'{env.serverUrl}/{BAYMARKETS_MARGIN_CALC}',
                                   json=data_dict,
                                   auth = BasicAuth(env.username, env.password)) as resp:
                json_resp = await resp.json()
                LOG.info(f'Integration <== Exberry: {json_resp}')
                if 'error' in json_resp:
                    commands.append(create_error_response(json_resp))
                else:
                    commands.append(create(BAYMARKETS.MarginCalculationResponse, {
                        'integrationParty': env.party,
                        'calculationId': json_resp['calculationId'],
                    }))
        return commands

    @events.ledger.contract_created(BAYMARKETS.RequestMarkToMarketCalculation)
    async def handle_request_mark_to_market(event):
        commands = [exercise(event.cid, 'Archive', {})]
        async with ClientSession() as session:
            data_dict = {}
            LOG.info(f'Integration ==> Baymarkets: GET {data_dict}')
            async with session.post(url = f'{env.serverUrl}/{BAYMARKETS_MARK_TO_MARKET_CALC}',
                                   json=data_dict,
                                   auth = BasicAuth(env.username, env.password)) as resp:
                json_resp = await resp.json()
                LOG.info(f'Integration <== Exberry: {json_resp}')
                if 'error' in json_resp:
                    commands.append(create_error_response(json_resp))
                else:
                    commands.append(create(BAYMARKETS.MarkToMarketCalculationResponse, {
                        'integrationParty': env.party,
                        'calculationId': json_resp['calculationId'],
                    }))
        return commands

    def create_error_response(json_resp: dict):
        return create(BAYMARKETS.ErrorResponse, {
            'integrationParty': env.party,
            'timestamp': json_resp['timestamp'],
            'status': int(json_resp['status']),
            'error': json_resp['error'],
            'message': json_resp['message'],
            'path': json_resp['path']
        })
