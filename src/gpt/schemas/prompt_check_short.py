schema = {
    "type": "object",
    "properties": {
        "is_valid": {
            "type": "boolean"
        },
        "position_tags_cloud": {
            "type": "array",
            "items": {
                "type": "string",
            },
        },
        "eli5": {
            "type": "string"
        }
    },
    "required": [
        "is_valid",
        "position_tags_cloud",
        "eli5"
    ]
}
