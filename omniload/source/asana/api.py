from urllib.parse import parse_qs, urlparse

import dlt


class AsanaSource:
    resources = [
        "workspaces",
        "projects",
        "sections",
        "tags",
        "tasks",
        "stories",
        "teams",
        "users",
    ]

    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        workspace = parsed_uri.hostname
        access_token = params.get("access_token")

        if not workspace:
            raise ValueError("workspace ID must be specified in the URI")

        if not access_token:
            raise ValueError("access_token is required for connecting to Asana")

        if table not in self.resources:
            raise ValueError(
                f"Resource '{table}' is not supported for Asana source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        from omniload.source.asana.adapter import asana_source

        dlt.secrets["sources.asana.access_token"] = access_token[0]

        src = asana_source()
        src.workspaces.add_filter(lambda w: w["gid"] == workspace)
        return src.with_resources(table)
