"""
Exceptions to be raised in case of issues or unexpected behaviours
of dbt Cloud.
"""


class DbtCloudBaseException(Exception):
    """Base exception for all dbt Cloud errors"""

    pass


class DbtCloudConfigurationException(Exception):
    """Raise when dbt Cloud client is misconfigured"""

    pass


class DbtCloudCreateJobFailed(DbtCloudBaseException):
    """Raise when job creation fails"""

    pass


class TriggerDbtCloudRunFailed(DbtCloudBaseException):
    """Raised when triggering a dbt job run fails"""

    pass


class GetDbtCloudRunFailed(DbtCloudBaseException):
    """Raised when details for a dbt Cloud job run cannot be retrieved"""

    pass


class DbtCloudRunFailed(DbtCloudBaseException):
    """Raised when a dbt Cloud run fails"""

    pass


class DbtCloudRunCanceled(DbtCloudBaseException):
    """Raised when a dbt Cloud run has been canceled before completion"""

    pass


class DbtCloudRunTimedOut(DbtCloudBaseException):
    """Raised when a dbt Cloud run does not complete in the provided time"""

    pass


class DbtCloudListArtifactsFailed(DbtCloudBaseException):
    """Raised when dbt Cloud artifacts cannot be listed"""

    pass
