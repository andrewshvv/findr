class IntegrityCheck(Exception):
    pass


class CorruptedAIResponse(Exception):
    pass


class TokenLimitExceeded(Exception):
    def __init__(self, num_tokens):
        self.num_tokens = num_tokens
