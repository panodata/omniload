import pendulum


class GoogleAnalyticsSource:
    def handles_incrementality(self) -> bool:
        return True

    def dlt_source(self, uri: str, table: str, **kwargs):
        import omniload.source.google_analytics.helpers as helpers

        if kwargs.get("incremental_key"):
            raise ValueError(
                "Google Analytics takes care of incrementality on its own, you should not provide incremental_key"
            )

        result = helpers.parse_google_analytics_uri(uri)
        credentials = result["credentials"]
        property_ids = result["property_ids"]

        fields = table.split(":")
        if len(fields) != 3 and len(fields) != 4:
            raise ValueError(
                "Invalid table format. Expected format: <report_type>:<dimensions>:<metrics> or <report_type>:<dimensions>:<metrics>:<minute_ranges>"
            )

        report_type = fields[0]
        if report_type not in ["custom", "realtime"]:
            raise ValueError(
                "Invalid report type. Expected format: <report_type>:<dimensions>:<metrics>. Available report types: custom, realtime"
            )

        dimensions = fields[1].replace(" ", "").split(",")
        metrics = fields[2].replace(" ", "").split(",")

        minute_range_objects = []
        if len(fields) == 4:
            minute_range_objects = (
                helpers.convert_minutes_ranges_to_minute_range_objects(fields[3])
            )

        datetime = ""
        resource_name = fields[0].lower()
        if resource_name == "custom":
            for dimension_datetime in ["date", "dateHourMinute", "dateHour"]:
                if dimension_datetime in dimensions:
                    datetime = dimension_datetime
                    break
            else:
                raise ValueError(
                    "You must provide at least one dimension: [dateHour, dateHourMinute, date]"
                )

        queries = [
            {
                "resource_name": resource_name,
                "dimensions": dimensions,
                "metrics": metrics,
            }
        ]

        start_date = pendulum.now().subtract(days=30).start_of("day")
        if kwargs.get("interval_start") is not None:
            start_date = pendulum.instance(kwargs.get("interval_start"))  # ty: ignore[no-matching-overload]

        end_date = pendulum.now()
        if kwargs.get("interval_end") is not None:
            end_date = pendulum.instance(kwargs.get("interval_end"))  # ty: ignore[no-matching-overload]

        from omniload.source.google_analytics.adapter import google_analytics

        return google_analytics(
            property_ids=property_ids,
            start_date=start_date,
            end_date=end_date,
            datetime_dimension=datetime,
            queries=queries,
            credentials=credentials,
            minute_range_objects=minute_range_objects if minute_range_objects else None,
        ).with_resources(resource_name)
