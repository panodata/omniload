from urllib.parse import parse_qs, urlparse

from dlt.extract.exceptions import ResourcesNotFoundError

from omniload.error import MissingValueError, UnsupportedResourceError


class MailchimpSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        query_params = parse_qs(parsed_uri.query)
        api_key = query_params.get("api_key")
        server = query_params.get("server")

        if api_key is None:
            raise MissingValueError("api_key", "Mailchimp")
        if server is None:
            raise MissingValueError("server", "Mailchimp")

        from omniload.source.mailchimp.adapter import mailchimp_source

        try:
            return mailchimp_source(
                api_key=api_key[0],
                server=server[0],
            ).with_resources(table)
        except ResourcesNotFoundError:
            raise UnsupportedResourceError(table, "Mailchimp")
