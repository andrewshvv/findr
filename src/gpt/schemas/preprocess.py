schema = {
    "type": "object",
    "properties": {
        "category": {
            "type": "string",
            "enum": [
                "one_job_description",
                "multiple_jobs_opportunities",
                "scholarship",
                "hackathon",
                "resume",
                "study_program",
                "sprint_event",
                "advertisement"
            ],
        },
        "closed": {"type": "boolean"},
        "language": {"type": "string", "enum": ["en", "ru"]},
        "external": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "description": {"type": "string"},
                    "link_text": {"type": "string"},
                    "type": {"type": "string",
                             "enum": [
                                 "contact_phone",
                                 "project",
                                 "contact_email",
                                 "contact_telegram",
                                 "channel",
                                 "form",
                                 "job_link_on_company_website",
                                 "company", "job_platform"]},
                    "link": {"type": "string"}
                },
                "required": ["description", "link_text", "type", "link"]
            },
        },
        "section_headers": {
            "type": "array",
            "items": {"type": "string"},
        }
    },
    "required": [
        "category",
        "closed",
        "language",
        "external",
        "section_headers"
    ]
}
