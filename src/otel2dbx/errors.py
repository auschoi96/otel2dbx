class OTel2DbxError(RuntimeError):
    """Base error with an operator-actionable message."""


class ConfigurationError(OTel2DbxError):
    pass


class SourceError(OTel2DbxError):
    pass


class DestinationError(OTel2DbxError):
    pass


class VerificationError(OTel2DbxError):
    pass
