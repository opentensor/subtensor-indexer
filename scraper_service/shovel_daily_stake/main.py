import logging
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.clickhouse.batch_insert import buffer_insert
from shared.block_metadata import get_block_metadata
import rust_bindings

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map"

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64 CODEC(Delta, ZSTD),
            timestamp DateTime CODEC(Delta, ZSTD),
            hotkey String CODEC(ZSTD),
            coldkey String CODEC(ZSTD),
            stake UInt64 CODEC(Delta, ZSTD)
        ) ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (coldkey, hotkey, timestamp)
        """
        get_clickhouse_client().execute(query)

    (block_timestamp, block_hash) = get_block_metadata(n)
    results = rust_bindings.query_block_stakes(block_hash)
    print(f"Processing block {n}. Found {len(results)} stake entries")
    for result in results:
        hotkey = result[0]
        coldkey, stake = result[1][0]
        buffer_insert(
            table_name,
            [n, block_timestamp, f"'{hotkey}'", f"'{coldkey}'", stake]
        )


def main():
    StakeDailyMapShovel(name="stake_daily_map", skip_interval=7200).start()


if __name__ == "__main__":
    main()
