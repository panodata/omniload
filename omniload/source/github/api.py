from typing import cast
from urllib.parse import parse_qs, urlparse

import pendulum


class GitHubSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Github takes care of incrementality on its own, you should not provide incremental_key"
            )
        # github://?access_token=<access_token>&owner=<owner>&repo=<repo>
        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)

        owner = source_fields.get("owner", [None])[0]
        if not owner:
            raise ValueError(
                "owner of the repository is required to connect with GitHub"
            )

        repo = source_fields.get("repo", [None])[0]
        if not repo:
            raise ValueError(
                "repo variable is required to retrieve data for a specific repository from GitHub."
            )

        access_token = source_fields.get("access_token", [""])[0]

        from omniload.source.github.adapter import (
            github_reactions,
            github_repo_events,
            github_stargazers,
        )

        if table in ["issues", "pull_requests"]:
            return github_reactions(
                owner=owner, name=repo, access_token=access_token
            ).with_resources(table)
        elif table == "repo_events":
            start_date = kwargs.get("interval_start") or pendulum.now().subtract(
                days=30
            )
            end_date = kwargs.get("interval_end") or None

            start_dt: pendulum.DateTime
            end_dt: pendulum.DateTime
            if isinstance(start_date, str):
                start_dt = cast(pendulum.DateTime, pendulum.parse(start_date))
            else:
                start_dt = cast(pendulum.DateTime, start_date)
            if isinstance(end_date, str):
                end_dt = cast(pendulum.DateTime, pendulum.parse(end_date))
            else:
                end_dt = cast(pendulum.DateTime, end_date)

            return github_repo_events(
                owner=owner,
                name=repo,
                access_token=access_token,
                start_date=start_dt,
                end_date=end_dt,
            )
        elif table == "stargazers":
            return github_stargazers(owner=owner, name=repo, access_token=access_token)
        else:
            raise ValueError(
                f"Resource '{table}' is not supported for GitHub source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )
