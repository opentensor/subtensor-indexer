import json
from functools import lru_cache
from shared.clickhouse.utils import (
    escape_column_name,
    get_clickhouse_client,
    table_exists,
)


def format_value(value, column_type=None):
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        # SQL requires strings to be wrapped in single quotes
        return f"'{value}'"
    elif isinstance(value, list):
        if isinstance(column_type, str) and "Array" in column_type:
            return value
        else:
            return f"'{json.dumps(value)}'"
    else:
        return value


def get_column_type(value, value_type=None, key=None):
    if isinstance(value, str):
        return "String"
    elif isinstance(value, int):
        return "Int64"
    elif isinstance(value, float):
        return "Float64"
    elif isinstance(value, list):
        if len(value) > 0:
            inner = value[0]
            # Only use Array if the inner value will have a proper type, otherwise, just
            # stringify it.
            if (
                isinstance(inner, str)
                or isinstance(inner, int)
                or isinstance(inner, float)
            ):
                return f"Array({get_column_type(inner)})"
            else:
                return "String"
        elif value_type == "Vec<u8>":
            return "Array(UInt8)"
        elif value_type == "Vec<u16>":
            return "Array(UInt16)"
        elif value_type == "Vec<u32>":
            return "Array(UInt32)"
        elif value_type == "Vec<u64>":
            return "Array(UInt64)"
        elif key in type_map:
            return type_map[key]
        else:
            print(
                f"Empty list and don't know what column type to use for key {
                    key}!"
            )
            exit(1)
    elif value is None:
        return None
    else:
        print(f"Unhandled type: {type(value)}")
        return "String"


# map of arg types when they cannot be derived from the value
# maps the key to the type
type_map = {
    "info__additional": "Array(Tuple(String, String))",
    "info__legal__Raw0": "String",
    "info__riot__Raw0": "String",
    "info__image__Raw0": "String",
    "info__email__Raw0": "String",
    "info__twitter__Raw0": "String",
    "info__web__Raw0": "String",
    "calls": "Array(String)",
    "call__call_args": "Array(String)",
    "transaction__EIP1559__access_list": "Array(String)",
    "children": "Array(Tuple(UInt64, String))",
}


def generate_column_definitions(item, parent_key, item_type=None):
    column_names = []
    column_types = []
    values = []

    if isinstance(item, dict):
        for key, value in item.items():
            column_name = f"{parent_key}__{key}"
            (_column_names, _column_types, _values) = generate_column_definitions(
                value, column_name
            )
            column_names.extend(_column_names)
            column_types.extend(_column_types)
            values.extend(_values)
    elif isinstance(item, tuple):
        for i, item in enumerate(item):
            item_key = f"tuple_{i}"
            item_name = f"{parent_key}.{item_key}"
            (_column_names, _column_types, _values) = generate_column_definitions(
                item, item_name
            )
            column_names.extend(_column_names)
            column_types.extend(_column_types)
            values.extend(_values)
    else:
        column_type = get_column_type(item, item_type, parent_key)
        if column_type is not None:
            column_name = parent_key
            column_names.append(f"arg_{column_name}")
            column_types.append(column_type)
            values.append(format_value(item, column_type))

    return (column_names, column_types, values)


def create_clickhouse_table(table_name, column_names, column_types):
    columns = list(
        map(
            lambda x, y: f"{escape_column_name(x)} {
                y}",
            column_names,
            column_types,
        )
    )
    column_definitions = ", ".join(columns)

    order_by = ["call_module", "call_function", "timestamp", "extrinsic_index"]

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {column_definitions}
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY toYYYYMM(timestamp)
    ORDER BY ({", ".join(order_by)})
    """

    get_clickhouse_client().execute(sql)


@lru_cache(maxsize=None)
def get_table_name(module_id, function_id, columns):
    """
    Returns a unique table name for the extrinsic_id and its columns.

    Initializes the table if it doesn't yet exist.

    Multiple tables for the same event can exist when the schema of the extrinsic changes.

    'columns' must be passed as a tuple to be hashable.
    """
    extrinsic_id = f"{module_id}_{function_id}"
    client = get_clickhouse_client()

    version = 0

    # Allow up to 50 tables for the same event_id
    MAX_VERSIONS = 50

    while version < MAX_VERSIONS:
        table_name = f"shovel_extrinsics_{extrinsic_id}_v{version}"

        # If the stable doesn't exist, we will create it for this version of the extrinsic we are
        # processing
        if not table_exists(table_name):
            return table_name

        # If table exists, we need to check the schema at this version matches the extrinsic we're
        # currently processing
        else:
            query = f"DESCRIBE TABLE '{table_name}'"
            result = client.execute(query)
            different_version = False

            if len(result) != len(columns):
                different_version = True

            for i, column in enumerate(result):
                if different_version or column[0] != columns[i]:
                    different_version = True
                    break

            if different_version:
                version += 1
            else:
                return table_name

    logging.error(f"Max versions reached for extrinsic {extrinsic_id}")
