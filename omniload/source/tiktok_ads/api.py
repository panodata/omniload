from urllib.parse import parse_qs, urlparse

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc


class TikTokSource:
    # tittok://?access_token=<access_token>&advertiser_id=<advertiser_id>
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "TikTok takes care of incrementality on its own, you should not provide incremental_key"
            )

        endpoint = "custom_reports"

        parsed_uri = urlparse(uri)
        source_fields = parse_qs(parsed_uri.query)

        access_token = source_fields.get("access_token")
        if not access_token:
            raise ValueError("access_token is required to connect to TikTok")

        timezone = "UTC"
        if source_fields.get("timezone") is not None:
            timezone = source_fields.get("timezone")[0]  # type: ignore

        advertiser_ids = source_fields.get("advertiser_ids")
        if not advertiser_ids:
            raise ValueError("advertiser_ids is required to connect to TikTok")

        advertiser_ids = advertiser_ids[0].replace(" ", "").split(",")

        start_date = pendulum.now().subtract(days=30).in_tz(timezone)
        end_date = ensure_pendulum_datetime_utc(pendulum.now()).in_tz(timezone)

        interval_start = kwargs.get("interval_start")
        if interval_start is not None:
            start_date = ensure_pendulum_datetime_utc(interval_start).in_tz(timezone)

        interval_end = kwargs.get("interval_end")
        if interval_end is not None:
            end_date = ensure_pendulum_datetime_utc(interval_end).in_tz(timezone)

        page_size = min(1000, kwargs.get("page_size", 1000))

        if table.startswith("custom:"):
            fields = table.split(":", 3)
            if len(fields) != 3 and len(fields) != 4:
                raise ValueError(
                    "Invalid TikTok custom table format. Expected format: custom:<dimensions>,<metrics> or custom:<dimensions>:<metrics>:<filters>"
                )

            dimensions = fields[1].replace(" ", "").split(",")
            if (
                "campaign_id" not in dimensions
                and "adgroup_id" not in dimensions
                and "ad_id" not in dimensions
            ):
                raise ValueError(
                    "TikTok API requires at least one ID dimension, please use one of the following dimensions: [campaign_id, adgroup_id, ad_id]"
                )

            if "advertiser_id" in dimensions:
                dimensions.remove("advertiser_id")

            metrics = fields[2].replace(" ", "").split(",")
            filtering_param = False
            filter_name = ""
            filter_value = []
            if len(fields) == 4:

                def parse_filters(filters_raw: str) -> dict:
                    # Parse filter string like "key1=value1,key2=value2,value3,value4"
                    filters = {}
                    current_key = None

                    for item in filters_raw.split(","):
                        if "=" in item:
                            # Start of a new key-value pair
                            key, value = item.split("=")
                            filters[key] = [value]  # Always start with a list
                            current_key = key
                        elif current_key is not None:
                            # Additional value for the current key
                            filters[current_key].append(item)

                    # Convert single-item lists to simple values
                    return {k: v[0] if len(v) == 1 else v for k, v in filters.items()}

                filtering_param = True
                filters = parse_filters(fields[3])
                if len(filters) > 1:
                    raise ValueError(
                        "Only one filter is allowed for TikTok custom reports"
                    )
                filter_name = list(filters.keys())[0]
                filter_value = list(map(int, filters[list(filters.keys())[0]]))

        from omniload.source.tiktok_ads.adapter import tiktok_source

        return tiktok_source(
            start_date=start_date,
            end_date=end_date,
            access_token=access_token[0],
            advertiser_ids=advertiser_ids,
            timezone=timezone,
            dimensions=dimensions,
            metrics=metrics,
            page_size=page_size,
            filter_name=filter_name,
            filter_value=filter_value,
            filtering_param=filtering_param,
        ).with_resources(endpoint)
