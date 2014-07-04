class NsqException(Exception):
    pass


class NsqConnectGiveUpError(NsqException):
    pass


class NsqErrorResponseError(NsqException):
    pass
