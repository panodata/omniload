from urllib.parse import parse_qs, urlparse

import dlt


class JiraSource:
    resources = [
        "projects",
        "issues",
        "users",
        "issue_types",
        "statuses",
        "priorities",
        "resolutions",
        "project_versions",
        "project_components",
        "events",
        "issue_changelogs",
    ]

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        params = parse_qs(parsed_uri.query)

        base_url = f"https://{parsed_uri.netloc}"
        email = params.get("email")
        api_token = params.get("api_token")

        if not email:
            raise ValueError("email must be specified in the URI query parameters")

        if not api_token:
            raise ValueError("api_token is required for connecting to Jira")

        flags = {
            "skip_archived": False,
        }
        if ":" in table:
            table, rest = table.split(":", 1)
            for k in rest.split(":"):
                flags[k] = True

        if table not in self.resources:
            raise ValueError(
                f"Resource '{table}' is not supported for Jira source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        from omniload.source.jira.adapter import jira_source

        dlt.secrets["sources.jira.base_url"] = base_url
        dlt.secrets["sources.jira.email"] = email[0]
        dlt.secrets["sources.jira.api_token"] = api_token[0]

        src = jira_source()
        if flags["skip_archived"]:
            src.projects.add_filter(lambda p: not p.get("archived", False))
        return src.with_resources(table)
