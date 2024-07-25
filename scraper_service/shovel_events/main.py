from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    table_exists,
)
import logging

from shovel_events.utils import (
    create_clickhouse_table,
    generate_column_definitions,
    get_table_name,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class EventsShovel(ShovelBaseClass):
    def process_block(self, n):
        substrate = get_substrate_client()

        (block_timestamp, block_hash) = get_block_metadata(n)

        events = substrate.query(
            "System",
            "Events",
            block_hash=block_hash,
        )

        for e in events:
            event = e.value["event"]
            (column_names, column_types, values) = generate_column_definitions(
                event["attributes"]
            )

            table_name = get_table_name(
                event["module_id"], event["event_id"], tuple(column_names)
            )

            # Dynamically create table if not exists
            if not table_exists(table_name):
                create_clickhouse_table(
                    table_name, column_names, column_types, values)

            # Insert event data into table
            all_values = [n, block_timestamp] + values
            buffer_insert(table_name, all_values)


def main():
    EventsShovel(name="events").start()


if __name__ == "__main__":
    main()