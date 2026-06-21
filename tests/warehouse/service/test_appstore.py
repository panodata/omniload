import base64
import gzip
import io
from typing import Callable, Iterable
from unittest.mock import MagicMock, patch

import pytest
import requests
import sqlalchemy

from omniload.src.appstore import (
    AnalyticsReportInstancesResponse,
    NoOngoingReportRequestsFoundError,
    NoReportsFoundError,
    NoSuchReportError,
)
from omniload.src.appstore.models import (
    AnalyticsReportRequestsResponse,
    AnalyticsReportResponse,
    AnalyticsReportSegmentsResponse,
    Report,
    ReportAttributes,
    ReportInstance,
    ReportInstanceAttributes,
    ReportRequest,
    ReportRequestAttributes,
    ReportSegment,
    ReportSegmentAttributes,
)
from tests.util import get_random_string, has_exception, invoke_ingest_command
from tests.warehouse.container import DESTINATIONS


def appstore_test_cases() -> Iterable[Callable]:
    app_download_testdata = (
        "Date\tApp Apple Identifier\tCounts\tProcessing Date\tApp Name\tDownload Type\tApp Version\tDevice\tPlatform Version\tSource Type\tSource Info\tCampaign\tPage Type\tPage Title\tPre-Order\tTerritory\n"
        "2025-01-01\t1\t590\t2025-01-01\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.1\tApp Store search\t\t\tNo page\tNo page\t\tFR\n"
        "2025-01-01\t1\t16\t2025-01-01\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.1\tApp referrer\tcom.burbn.instagram\t\tStore sheet\tDefault custom product page\t\tSG\n"
        "2025-01-01\t1\t11\t2025-01-01\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.3\tApp Store search\t\t\tNo page\tNo page\t\tMX\n"
    )

    app_download_testdata_extended = (
        "Date\tApp Apple Identifier\tCounts\tProcessing Date\tApp Name\tDownload Type\tApp Version\tDevice\tPlatform Version\tSource Type\tSource Info\tCampaign\tPage Type\tPage Title\tPre-Order\tTerritory\n"
        "2025-01-02\t1\t590\t2025-01-02\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.1\tApp Store search\t\t\tNo page\tNo page\t\tFR\n"
        "2025-01-02\t1\t16\t2025-01-02\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.1\tApp referrer\tcom.burbn.instagram\t\tStore sheet\tDefault custom product page\t\tSG\n"
        "2025-01-02\t1\t11\t2025-01-02\tAcme Inc\tAuto-update\t4.2.40\tiPhone\tiOS 18.3\tApp Store search\t\t\tNo page\tNo page\t\tMX\n"
    )

    api_key = base64.b64encode(b"MOCK_KEY").decode()

    def create_mock_response(data: str) -> requests.Response:
        res = requests.Response()
        buffer = io.BytesIO()
        archive = gzip.GzipFile(fileobj=buffer, mode="w")
        archive.write(data.encode())
        archive.close()
        buffer.seek(0)
        res.status_code = 200
        res.raw = buffer
        return res

    def test_no_report_instances_found(dest_uri):
        """
        When there are no report instances for the given date range,
        NoReportsError should be raised.
        """
        client = MagicMock()
        client.list_analytics_report_requests = MagicMock(
            return_value=AnalyticsReportRequestsResponse(
                [
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="123",
                        attributes=ReportRequestAttributes(
                            accessType="ONGOING", stoppedDueToInactivity=False
                        ),
                    )
                ],
                None,
                None,
            )
        )
        client.list_analytics_reports = MagicMock(
            return_value=AnalyticsReportResponse(
                [
                    Report(
                        type="analyticsReports",
                        id="123",
                        attributes=ReportAttributes(
                            name="app-downloads-detailed", category="USER"
                        ),
                    )
                ],
                None,
                None,
            )
        )
        client.list_report_instances = MagicMock(
            return_value=AnalyticsReportInstancesResponse(
                [
                    ReportInstance(
                        type="analyticsReportInstances",
                        id="123",
                        attributes=ReportInstanceAttributes(
                            granularity="DAILY", processingDate="2024-01-03"
                        ),
                    )
                ],
                None,
                None,
            )
        )

        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}&app_id=123",
                "app-downloads-detailed",
                dest_uri,
                dest_table,
                interval_start="2024-01-01",
                interval_end="2024-01-02",
                print_output=False,
            )
            assert has_exception(result.exception, NoReportsFoundError)

    def test_no_ongoing_reports_found(dest_uri):
        """
        when there are no ongoing reports, or ongoing reports that have
        been stopped due to inactivity, NoOngoingReportRequestsFoundError should be raised.
        """
        client = MagicMock()
        client.list_analytics_report_requests = MagicMock(
            return_value=AnalyticsReportRequestsResponse(
                [
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="123",
                        attributes=ReportRequestAttributes(
                            accessType="ONE_TIME_SNAPSHOT", stoppedDueToInactivity=False
                        ),
                    ),
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="124",
                        attributes=ReportRequestAttributes(
                            accessType="ONGOING", stoppedDueToInactivity=True
                        ),
                    ),
                ],
                None,
                None,
            )
        )
        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}&app_id=123",
                "app-downloads-detailed",
                dest_uri,
                dest_table,
                interval_start="2024-01-01",
                interval_end="2024-01-02",
                print_output=False,
            )
            assert has_exception(result.exception, NoOngoingReportRequestsFoundError)

    def test_no_such_report(dest_uri):
        """
        when there is no report with the given name, NoSuchReportError should be raised.
        """
        client = MagicMock()
        client.list_analytics_report_requests = MagicMock(
            return_value=AnalyticsReportRequestsResponse(
                [
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="123",
                        attributes=ReportRequestAttributes(
                            accessType="ONGOING", stoppedDueToInactivity=False
                        ),
                    )
                ],
                None,
                None,
            )
        )
        client.list_analytics_reports = MagicMock(
            return_value=AnalyticsReportResponse(
                [],
                None,
                None,
            )
        )

        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
            dest_table = f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
            result = invoke_ingest_command(
                f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}&app_id=123",
                "app-downloads-detailed",
                dest_uri,
                dest_table,
                interval_start="2024-01-01",
                interval_end="2024-01-02",
                print_output=False,
            )
            assert has_exception(result.exception, NoSuchReportError)

    def test_successful_ingestion(dest_uri):
        """
        When there are report instances for the given date range, the data should be ingested
        """
        client = MagicMock()
        client.list_analytics_report_requests = MagicMock(
            return_value=AnalyticsReportRequestsResponse(
                [
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="123",
                        attributes=ReportRequestAttributes(
                            accessType="ONGOING", stoppedDueToInactivity=False
                        ),
                    )
                ],
                None,
                None,
            )
        )
        client.list_analytics_reports = MagicMock(
            return_value=AnalyticsReportResponse(
                [
                    Report(
                        type="analyticsReports",
                        id="123",
                        attributes=ReportAttributes(
                            name="app-downloads-detailed", category="USER"
                        ),
                    )
                ],
                None,
                None,
            )
        )

        client.list_report_instances = MagicMock(
            return_value=AnalyticsReportInstancesResponse(
                [
                    ReportInstance(
                        type="analyticsReportInstances",
                        id="123",
                        attributes=ReportInstanceAttributes(
                            granularity="DAILY", processingDate="2025-01-01"
                        ),
                    )
                ],
                None,
                None,
            )
        )

        client.list_report_segments = MagicMock(
            return_value=AnalyticsReportSegmentsResponse(
                [
                    ReportSegment(
                        type="analyticsReportSegments",
                        id="123",
                        attributes=ReportSegmentAttributes(
                            checksum="checksum-0",
                            url="http://example.com/report.csv",  # we'll monkey patch requests.get to return this file
                            sizeInBytes=123,
                        ),
                    )
                ],
                None,
                None,
            )
        )

        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            with patch("requests.get") as mock_get:
                mock_get.return_value = create_mock_response(app_download_testdata)
                schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
                dest_table = (
                    f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
                )
                result = invoke_ingest_command(
                    f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}",
                    "app-downloads-detailed:123",  # moved the app ID to the table name to ensure that also works
                    dest_uri,
                    dest_table,
                    interval_start="2025-01-01",
                    interval_end="2025-01-02",
                )

        assert result.exit_code == 0

        dest_engine = sqlalchemy.create_engine(dest_uri)
        dest_conn = dest_engine.connect()
        count = dest_conn.exec_driver_sql(
            f"select count(*) from {dest_table}"
        ).scalar_one()
        dest_engine.dispose()
        assert count == 3

    def test_incremental_ingestion(dest_uri):
        """
        when the pipeline is run till a specific end date, the next ingestion
        should load data from the last processing date, given that last_date is not provided
        """

        client = MagicMock()
        client.list_analytics_report_requests = MagicMock(
            return_value=AnalyticsReportRequestsResponse(
                [
                    ReportRequest(
                        type="analyticsReportRequests",
                        id="123",
                        attributes=ReportRequestAttributes(
                            accessType="ONGOING", stoppedDueToInactivity=False
                        ),
                    )
                ],
                None,
                None,
            )
        )
        client.list_analytics_reports = MagicMock(
            return_value=AnalyticsReportResponse(
                [
                    Report(
                        type="analyticsReports",
                        id="123",
                        attributes=ReportAttributes(
                            name="app-downloads-detailed", category="USER"
                        ),
                    )
                ],
                None,
                None,
            )
        )

        client.list_report_instances = MagicMock(
            return_value=AnalyticsReportInstancesResponse(
                [
                    ReportInstance(
                        type="analyticsReportInstances",
                        id="123",
                        attributes=ReportInstanceAttributes(
                            granularity="DAILY", processingDate="2025-01-01"
                        ),
                    ),
                    ReportInstance(
                        type="analyticsReportInstances",
                        id="123",
                        attributes=ReportInstanceAttributes(
                            granularity="DAILY", processingDate="2025-01-02"
                        ),
                    ),
                ],
                None,
                None,
            )
        )

        client.list_report_segments = MagicMock(
            return_value=AnalyticsReportSegmentsResponse(
                [
                    ReportSegment(
                        type="analyticsReportSegments",
                        id="123",
                        attributes=ReportSegmentAttributes(
                            checksum="checksum-0",
                            url="http://example.com/report.csv",  # we'll monkey patch requests.get to return this file
                            sizeInBytes=123,
                        ),
                    )
                ],
                None,
                None,
            )
        )

        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            with patch("requests.get") as mock_get:
                mock_get.return_value = create_mock_response(app_download_testdata)
                schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
                dest_table = (
                    f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
                )
                result = invoke_ingest_command(
                    f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}&app_id=123",
                    "app-downloads-detailed",
                    dest_uri,
                    dest_table,
                    interval_end="2025-01-01",
                )

        assert result.exit_code == 0

        dest_engine = sqlalchemy.create_engine(dest_uri)
        with dest_engine.connect() as dest_conn:
            count = dest_conn.exec_driver_sql(
                f"select count(*) from {dest_table}"
            ).scalar_one()
        dest_engine.dispose()
        assert count == 3

        # now run the pipeline again without an end date
        with patch("omniload.src.appstore.client.AppStoreConnectClient") as mock_client:
            mock_client.return_value = client
            with patch("requests.get") as mock_get:
                mock_get.side_effect = [
                    create_mock_response(app_download_testdata),
                    create_mock_response(app_download_testdata_extended),
                ]
                schema_rand_prefix = f"testschema_appstore_{get_random_string(5)}"
                dest_table = (
                    f"{schema_rand_prefix}.app_downloads_{get_random_string(5)}"
                )
                result = invoke_ingest_command(
                    f"appstore://?key_id=123&issuer_id=123&key_base64={api_key}&app_id=123",
                    "app-downloads-detailed",
                    dest_uri,
                    dest_table,
                )

        assert result.exit_code == 0

        dest_engine = sqlalchemy.create_engine(dest_uri)
        with dest_engine.connect() as dest_conn:
            count = dest_conn.exec_driver_sql(
                f"select count(*) from {dest_table}"
            ).scalar_one()
            assert count == 6
            assert (
                len(
                    dest_conn.exec_driver_sql(
                        f"select processing_date from {dest_table} group by 1"
                    ).fetchall()
                )
                == 2
            )
        dest_engine.dispose()

    return [
        test_no_report_instances_found,
        test_no_ongoing_reports_found,
        test_no_such_report,
        test_successful_ingestion,
        test_incremental_ingestion,
    ]


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
@pytest.mark.parametrize("test_case", appstore_test_cases())
def test_appstore(dest, test_case):
    test_case(dest.start())
    dest.stop()
