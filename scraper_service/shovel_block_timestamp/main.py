from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class BlockTimestampShovel(ShovelBaseClass):
    table_name = "shovel_block_timestamps"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = 4920351

    def process_block(self, n):
        do_process_block(self, n)


def do_process_block(self, n):
    try:
        substrate = get_substrate_client()

        try:
            if not table_exists(self.table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY block_number
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            block_hash = substrate.get_block_hash(n)
            block_timestamp = int(
                substrate.query(
                    "Timestamp",
                    "Now",
                    block_hash=block_hash,
                ).serialize()
                / 1000
            )
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block timestamp from substrate: {str(e)}")

        if block_timestamp == 0 and n != 0:
            raise ShovelProcessingError(f"Invalid block timestamp (0) for block {n}")

        try:
            buffer_insert(self.table_name, [n, block_timestamp])
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        # Convert unexpected exceptions to ShovelProcessingError
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def main():
    BlockTimestampShovel(name="block_timestamps").start()


if __name__ == "__main__":
    main()
