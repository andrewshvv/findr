schema = {
    "type": "object",
    "properties": {
        "reason": {
            "type": "string"
        },
        "score": {
            "type": "number"
        },
        "user_requirements": {
            "type": "array",
            "items": {
                "type": "string"

            },
        },
    },
    "required": ["user_requirements", "reason", "score"],
}
