from typing import Any
from urllib.parse import parse_qs, urlparse

from omniload.error import UnsupportedResourceError


class SnapchatAdsSource:
    resources = [
        "organizations",
        "fundingsources",
        "billingcenters",
        "adaccounts",
        "invoices",
        "transactions",
        "members",
        "roles",
        "campaigns",
        "adsquads",
        "ads",
        "event_details",
        "creatives",
        "segments",
        "campaigns_stats",
        "ad_accounts_stats",
        "ads_stats",
        "ad_squads_stats",
    ]

    # Resources that support ad_account_id filtering
    AD_ACCOUNT_RESOURCES = {
        "invoices",
        "campaigns",
        "adsquads",
        "ads",
        "event_details",
        "creatives",
        "segments",
    }

    # Stats resources
    STATS_RESOURCES = {
        "campaigns_stats",
        "ad_accounts_stats",
        "ads_stats",
        "ad_squads_stats",
    }

    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)

        refresh_token = source_fields.get("refresh_token")
        if not refresh_token:
            raise ValueError("refresh_token is required to connect to Snapchat Ads")

        client_id = source_fields.get("client_id")
        if not client_id:
            raise ValueError("client_id is required to connect to Snapchat Ads")

        client_secret = source_fields.get("client_secret")
        if not client_secret:
            raise ValueError("client_secret is required to connect to Snapchat Ads")

        organization_id = source_fields.get("organization_id")

        # Parse table name
        stats_config = None
        ad_account_id = None

        if ":" in table:
            parts = table.split(":")
            resource_name = parts[0]

            if resource_name in self.STATS_RESOURCES:
                # Stats table format parsed in helpers
                from omniload.source.snapchat_ads.helpers import parse_stats_table

                parsed = parse_stats_table(table)
                resource_name = parsed.resource_name
                # Build stats_config dict from ParsedStatsTable
                stats_config = {
                    "granularity": parsed.granularity,
                    "fields": parsed.fields,
                }
                if parsed.breakdown:
                    stats_config["breakdown"] = parsed.breakdown
                if parsed.dimension:
                    stats_config["dimension"] = parsed.dimension
                if parsed.pivot:
                    stats_config["pivot"] = parsed.pivot
            else:
                # Non-stats table with ad_account_id(s): resource_name:id1,id2,id3
                ad_account_ids_str = parts[1] if len(parts) > 1 else None
                if not ad_account_ids_str:
                    raise ValueError(
                        f"ad_account_id must be provided in format '{resource_name}:ad_account_id' or '{resource_name}:id1,id2,id3'"
                    )
                ad_account_id = [
                    _id.strip()
                    for _id in ad_account_ids_str.split(",")
                    if _id.strip()  # noqa: A001
                ]
        else:
            resource_name = table
            if resource_name in self.STATS_RESOURCES:
                # Stats resource with default config
                stats_config = {
                    "granularity": "DAY",
                    "fields": "impressions,spend",
                }

        # Validation for non-stats resources
        if resource_name not in self.STATS_RESOURCES:
            account_id_required = (
                resource_name in self.AD_ACCOUNT_RESOURCES
                and ad_account_id is None
                and not organization_id
            )
            if account_id_required:
                raise ValueError(
                    f"organization_id is required for '{resource_name}' table when no specific ad_account_id is provided"
                )

            if not organization_id and table != "organizations":
                raise ValueError(
                    f"organization_id is required for table '{table}'. Only 'organizations' table does not require organization_id."
                )
        else:
            # Stats resources require organization_id
            if not organization_id:
                raise ValueError(f"organization_id is required for '{resource_name}'")

        if resource_name not in self.resources:
            raise UnsupportedResourceError(table, "Snapchat Ads")

        from omniload.source.snapchat_ads.adapter import snapchat_ads_source

        source_kwargs: dict[str, Any] = {
            "refresh_token": refresh_token[0],
            "client_id": client_id[0],
            "client_secret": client_secret[0],
        }

        if organization_id:
            source_kwargs["organization_id"] = organization_id[0]

        # Only pass ad_account_id for non-stats resources
        if ad_account_id and resource_name not in self.STATS_RESOURCES:
            source_kwargs["ad_account_id"] = ad_account_id

        # Add interval_start and interval_end for client-side filtering
        interval_start = kwargs.get("interval_start")
        if interval_start:
            source_kwargs["start_date"] = interval_start

        interval_end = kwargs.get("interval_end")
        if interval_end:
            source_kwargs["end_date"] = interval_end

        # Add stats_config for stats resource
        if stats_config:
            source_kwargs["stats_config"] = stats_config

        source = snapchat_ads_source(**source_kwargs)

        return source.with_resources(resource_name)
