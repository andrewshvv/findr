class ParserException(Exception):
    def __init__(self, url, parser):
        self.url = url
        self.parser = parser


class JobArchived(ParserException):
    pass


class LoginRequired(ParserException):
    pass


class NotFound(ParserException):
    pass


class NotSupported(ParserException):
    pass
