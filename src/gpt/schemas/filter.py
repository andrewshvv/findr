schema = {
    "type": "object",
    "properties": {
        "reason": {
            "type": "string"
        },
        "query_result": {
            "type": "boolean"
        },
        "executed_query": {
            "type": "object"
        }
    },
    "required": ["reason", "query_result", "executed_query"],
}
