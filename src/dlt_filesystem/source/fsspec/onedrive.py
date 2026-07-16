from dlt_filesystem.source.fsspec.sharepoint import SharePointSource


class OneDriveSource(SharePointSource):
    """
    Access files on Microsoft SharePoint or OneDrive.

    https://github.com/acsone/msgraphfs
    """

    @property
    def fs_name(self):
        return "OneDrive"
