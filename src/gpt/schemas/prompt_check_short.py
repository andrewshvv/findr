schema = {
    "type": "object",
    "properties": {
        "language": {
            "type": "string"
        },
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
        "language",
        "is_valid",
        "position_tags_cloud",
        "eli5"
    ]
}
