from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
from typing import Dict, List, Any, Optional
from functools import lru_cache
from typing import Union
from scalecodec.utils.ss58 import ss58_encode

SS58_FORMAT = 42

def decode_account_id(account_id_bytes: Union[tuple[int], tuple[tuple[int]]]):
    if isinstance(account_id_bytes, tuple) and isinstance(account_id_bytes[0], tuple):
        account_id_bytes = account_id_bytes[0]
    return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)

logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s %(process)d %(message)s")

COMPOUNDING_PERIODS_PER_DAY = 7200

def create_validators_table(table_name):
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64,
            timestamp DateTime,
            name String,
            address String,
            image Nullable(String),
            description Nullable(String),
            owner Nullable(String),
            url Nullable(String),
            nominators UInt64,
            daily_return Float64,
            registrations Array(UInt64),
            validator_permits Array(UInt64),
            apy Nullable(Float64),
            subnet_hotkey_alpha Map(UInt64, Float64)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (block_number, address)
        """

        get_clickhouse_client().execute(query)

def calculate_apy_from_daily_return(return_per_1000: float, compounding_periods: int = COMPOUNDING_PERIODS_PER_DAY) -> float:
    daily_return = return_per_1000 / 1000
    apy = ((1 + (daily_return / compounding_periods)) ** (compounding_periods * 365)) - 1
    return round(apy * 100, 3)

@lru_cache
def create_storage_key_cached(pallet, storage, args):
    return get_substrate_client().create_storage_key(pallet, storage, list(args))

def get_subnet_uids(substrate, block_hash: str) -> List[int]:
    try:
        result = substrate.runtime_call(
            api="SubnetInfoRuntimeApi",
            method="get_subnets_info",
            params=[]
        )
        subnet_info = result
        return [info['netuid'] for info in subnet_info if 'netuid' in info]
    except Exception as e:
        logging.error(f"Failed to get subnet UIDs: {str(e)}")
        return []

def get_active_validators(substrate, block_hash: str) -> List[str]:
    try:
        result = substrate.runtime_call(
            api="DelegateInfoRuntimeApi",
            method="get_delegates",
            params=[]
        )

        delegate_info = result.value
        return [decode_account_id(delegate['delegate_ss58']) for delegate in delegate_info if 'delegate_ss58' in delegate]
    except Exception as e:
        logging.error(f"Failed to get active validators: {str(e)}")
        return []

def is_registered_in_subnet(substrate, net_uid: int, address: str) -> bool:
    try:
        key = create_storage_key_cached("subtensorModule", "uids", [net_uid, address])
        result = substrate.query_map(key)
        return bool(result)
    except Exception as e:
        logging.error(f"Failed to check subnet registration for {address} in subnet {net_uid}: {str(e)}")
        return False

def get_total_hotkey_alpha(substrate, address: str, net_uid: int) -> float:
    try:
        key = create_storage_key_cached("subtensorModule", "totalHotkeyAlpha", [address, net_uid])
        result = substrate.query_map(key)
        return float(str(result[0])) if result else 0.0
    except Exception as e:
        logging.error(f"Failed to get total hotkey alpha for {address} in subnet {net_uid}: {str(e)}")
        return 0.0

def fetch_validator_info(substrate, address: str, block_hash: str) -> Dict[str, Any]:
    try:
        result = substrate.runtime_call(
            api="DelegateInfoRuntimeApi",
            method="get_delegates",
            params=[]
        )
        delegate_info = result
        chain_info = next((d for d in delegate_info if d['delegate_ss58'] == address), None)

        if not chain_info:
            return {
                "name": address,
                "image": None,
                "description": None,
                "owner": None,
                "url": None
            }

        owner = chain_info.get('ownerSs58')
        if owner:
            key = create_storage_key_cached("subtensorModule", "identitiesV2", [owner])
            identity_bytes = substrate.query_map(key)
            identity = identity_bytes[0].decode() if identity_bytes else None
        else:
            identity = None

        return {
            "name": identity.get('name', address) if identity else address,
            "image": identity.get('image') if identity else None,
            "description": identity.get('description') if identity else None,
            "owner": owner,
            "url": identity.get('url') if identity else None
        }
    except Exception as e:
        logging.error(f"Failed to fetch validator info for {address}: {str(e)}")
        return {
            "name": address,
            "image": None,
            "description": None,
            "owner": None,
            "url": None
        }

def fetch_validator_stats(substrate, address: str, block_hash: str) -> Dict[str, Any]:
    try:
        result = substrate.runtime_call(
            api="DelegateInfoRuntimeApi",
            method="get_delegates",
            params=[]
        )
        delegate_info = result
        info = next((d for d in delegate_info if d['delegate_ss58'] == address), None)

        if not info:
            return {
                "nominators": 0,
                "daily_return": 0.0,
                "registrations": [],
                "validator_permits": [],
                "apy": None,
                "subnet_hotkey_alpha": {}
            }

        return_per_1000 = int(info['returnPer1000'], 16) if isinstance(info['returnPer1000'], str) else info['returnPer1000']
        apy = calculate_apy_from_daily_return(return_per_1000)

        subnet_uids = get_subnet_uids(substrate, block_hash)
        subnet_hotkey_alpha = {}

        for net_uid in subnet_uids:
            if is_registered_in_subnet(substrate, net_uid, address):
                alpha = get_total_hotkey_alpha(substrate, address, net_uid)
                if alpha > 0:
                    subnet_hotkey_alpha[net_uid] = alpha

        return {
            "nominators": len(info.get('nominators', [])),
            "daily_return": info.get('totalDailyReturn', 0.0),
            "registrations": info.get('registrations', []),
            "validator_permits": info.get('validatorPermits', []),
            "apy": apy,
            "subnet_hotkey_alpha": subnet_hotkey_alpha
        }
    except Exception as e:
        logging.error(f"Failed to fetch validator stats for {address}: {str(e)}")
        return {
            "nominators": 0,
            "daily_return": 0.0,
            "registrations": [],
            "validator_permits": [],
            "apy": None,
            "subnet_hotkey_alpha": {}
        }

class ValidatorsShovel(ShovelBaseClass):
    table_name = "shovel_validators"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 4920351

    def process_block(self, n):
        try:
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)

            create_validators_table(self.table_name)
            validators = get_active_validators(substrate, block_hash)

            for validator_address in validators:
                try:
                    info = fetch_validator_info(substrate, validator_address, block_hash)
                    stats = fetch_validator_stats(substrate, validator_address, block_hash)

                    values = [
                        n,
                        block_timestamp,
                        info["name"],
                        validator_address,
                        info["image"],
                        info["description"],
                        info["owner"],
                        info["url"],
                        stats["nominators"],
                        stats["daily_return"],
                        stats["registrations"],
                        stats["validator_permits"],
                        stats["apy"],
                        stats["subnet_hotkey_alpha"]
                    ]

                    print(values)

                    buffer_insert(self.table_name, values)

                except Exception as e:
                    logging.error(f"Error processing validator {validator_address}: {str(e)}")
                    continue

        except DatabaseConnectionError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")

def main():
    ValidatorsShovel(name="validators").start()

if __name__ == "__main__":
    main()
