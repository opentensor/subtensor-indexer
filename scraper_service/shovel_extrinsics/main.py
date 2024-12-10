from tenacity import retry, stop_after_attempt, wait_fixed

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client, reconnect_substrate
import logging

from shovel_extrinsics.utils import (
    create_clickhouse_table,
    format_value,
    generate_column_definitions,
    get_table_name,
)


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")


class ExtrinsicsShovel(ShovelBaseClass):
    def process_block(self, n):
        do_process_block(n)


def main():
    ExtrinsicsShovel(name="extrinsics").start()


@retry(
    wait=wait_fixed(2),
    before_sleep=lambda _: reconnect_substrate(),
    stop=stop_after_attempt(15)
)
def do_process_block(n):
    substrate = get_substrate_client()

    (block_timestamp, block_hash) = get_block_metadata(n)
    extrinsics = substrate.get_extrinsics(block_number=n)

    events = substrate.query(
        "System",
        "Events",
        block_hash=block_hash,
    )

    extrinsics_success_map = {}

    for e in events:
        event = e.value
        # Skip irrelevant events
        if event["event"]["module_id"] != "System" or (event["event"]["event_id"] != "ExtrinsicSuccess" and event["event"]["event_id"] != "ExtrinsicFailed"):
            continue

        extrinsics_success_map[int(event["extrinsic_idx"])
                               ] = event["event"]["event_id"] == "ExtrinsicSuccess"

    # Needed to handle edge case of duplicate events in the same block
    extrinsic_id = 0
    for e in extrinsics:
        extrinsic = e.value
        address = extrinsic.get("address", None)
        nonce = extrinsic.get("nonce", None)
        tip = extrinsic.get("tip", None)
        call_function = extrinsic["call"]["call_function"]
        call_module = extrinsic["call"]["call_module"]

        base_column_names = ["block_number", "timestamp", "extrinsic_index",
                             "call_function", "call_module", "success", "address", "nonce", "tip"]
        base_column_types = ["UInt64", "DateTime", "UInt64", "String",
                             "String", "Bool", "Nullable(String)", "Nullable(UInt64)", "Nullable(UInt64)"]

        base_column_values = [format_value(value) for value in [
            n, block_timestamp, extrinsic_id, call_function, call_module, extrinsics_success_map[extrinsic_id], address, nonce, tip]]

        arg_column_names = []
        arg_column_types = []
        arg_values = []
        for arg in extrinsic["call"]["call_args"]:
            (_arg_column_names, _arg_column_types, _arg_values) = generate_column_definitions(
                arg["value"], arg["name"], arg["type"]
            )
            arg_column_names.extend(_arg_column_names)
            arg_column_types.extend(_arg_column_types)
            arg_values.extend(_arg_values)

        column_names = base_column_names + arg_column_names
        column_types = base_column_types + arg_column_types
        values = base_column_values + arg_values

        table_name = get_table_name(
            call_module, call_function, tuple(column_names)
        )

        # Dynamically create table if not exists
        if not table_exists(table_name):
            create_clickhouse_table(
                table_name, column_names, column_types
            )

        buffer_insert(table_name, values)
        extrinsic_id += 1

    if len(extrinsics_success_map) != extrinsic_id:
        logging.error(
            f"Expected {len(extrinsics_success_map)} extrinsics, but only found {extrinsic_id}")
        exit(1)


if __name__ == "__main__":
    main()
