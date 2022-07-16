# Structure for creation table
# Diveded into separate file for flexible
# but changes in schema are not recommended

ERROR_EVENTS = {
    # str compile into format <key> <value>
    # so value can be not only type but also can contain flags
    "timestamp": "integer NOT NULL",
    "player_id": "integer",
    "error_id": "text",
    "json_server": "text",
    "json_client": "text"
}
