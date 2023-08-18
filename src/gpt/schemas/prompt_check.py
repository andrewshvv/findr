schema = {
    "type": "object",
    "properties": {
        "is_a_job_search_request": {
            "type": "boolean"
        },
        "position_tags_cloud": {
            "type": "array",
            "items": {
                "type": "string",
            },
        },
        "query": {
            "type": "object"
        }
    },
    "required": [
        "is_a_job_search_request",
        "position_tags_cloud",
        "query"
    ]
}
