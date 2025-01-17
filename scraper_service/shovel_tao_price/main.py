import logging

from cmc_client import get_price_by_time, CMC_TOKEN
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from tenacity import retry, wait_fixed

BLOCKS_A_DAY = (24 * 60 * 60) / 12
FETCH_EVERY_N_BLOCKS = (60 * 5) / 12;

# After this block change the interval from daily to every 5 mins
THRESHOLD_BLOCK = 4249779

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class TaoPriceShovel(ShovelBaseClass):
    table_name = "shovel_tao_price"
    starting_block=2137

    def process_block(self, n):
        logging.info("0. processing block...")
        # `skip_interval` has a hiccup sometimes
        # for unknown reasons and its not elastic
        # enough to handle conditions
        if n > THRESHOLD_BLOCK:
            logging.info("1")
            if n % FETCH_EVERY_N_BLOCKS != 0:
                logging.info("2")
                return
        else:
            logging.info("3")
            if n % BLOCKS_A_DAY != 0:
                logging.info("4")
                return
        logging.info("5. processing.")
        do_process_block(n, self.table_name)

@retry(
    wait=wait_fixed(30),
)
def do_process_block(n, table_name):
    logging.info(f"starting block {n}")
    substrate = get_substrate_client()

    logging.info("got substrate client")
    if not table_exists(table_name):
        logging.info("no table....")
        first_run(table_name)

    logging.info("got table")
    block_hash = substrate.get_block_hash(n)
    logging.info(f"getting block hash {block_hash}")
    block_timestamp = int(
        substrate.query(
            "Timestamp",
            "Now",
            block_hash=block_hash,
        ).serialize()
        / 1000
    )

    logging.info(f"block timestamp {block_timestamp}")

    if block_timestamp == 0:
        logging.info(f"timestamp is 0")
        return

    logging.info("getting price")
    latest_price_data = get_price_by_time(block_timestamp)
    logging.info("got price data")

    if latest_price_data:
        logging.info("and it is here")
        buffer_insert(table_name, [block_timestamp, *latest_price_data])
    else:
        raise Exception("Rate limit error encountered. Waiting before retrying...")




def first_run(table_name):
    query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp DateTime CODEC(Delta, ZSTD),
        price Float64 CODEC(ZSTD),
        market_cap Float64 CODEC(ZSTD),
        volume Float64 CODEC(ZSTD)
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY timestamp
    """
    get_clickhouse_client().execute(query)


def main():
    if not CMC_TOKEN:
        logging.error("CMC_TOKEN is not set. Doing nothing...")
    else:
        TaoPriceShovel(name="tao_price").start()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("An error occurred: %s", e)
        exit(1)
