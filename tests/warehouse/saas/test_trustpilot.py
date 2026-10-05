from unittest.mock import MagicMock, patch

import pytest

from tests.util import invoke_ingest_command
from tests.util.db import get_query_result
from tests.warehouse.settings import DESTINATIONS


def trustpilot_test_case(dest_uri):
    if dest_uri.startswith("cratedb://"):
        pytest.skip(
            "Fails on CrateDB with `DestinationSchemaTampered`, see "
            "https://github.com/crate/dlt-cratedb/issues/14"
        )
    sample_response = {
        "links": [
            {
                "href": "<Url for the resource>",
                "method": "<Http method for the resource>",
                "rel": "<Description of the relation>",
            }
        ],
        "reviews": [
            {
                "id": 1,
                "stars": 0,
                "title": None,
                "text": None,
                "language": None,
                "createdAt": "2023-01-01T12:00:00Z",
                "experiencedAt": "2023-01-01T12:00:00Z",
                "updatedAt": "2023-01-01T12:00:00Z",
                "numberOfLikes": 0,
                "isVerified": False,
                "status": None,
                "companyReply": {
                    "text": "This is our reply.",
                    "createdAt": "2013-09-07T13:37:00",
                    "updatedAt": "2013-09-07T13:37:00",
                },
                "consumer": {
                    "displayLocation": "Frederiksberg, DK",
                    "numberOfReviews": 1,
                    "displayName": "John Doe",
                    "id": "507f191e810c19729de860ea",
                    "links": [
                        {
                            "href": "<Url for the resource>",
                            "method": "<Http method for the resource>",
                            "rel": "<Description of the relation>",
                        }
                    ],
                },
                "businessUnit": {
                    "identifyingName": "trustpilot.com",
                    "displayName": "Trustpilot",
                    "id": "507f191e810c19729de860ea",
                    "links": [
                        {
                            "href": "<Url for the resource>",
                            "method": "<Http method for the resource>",
                            "rel": "<Description of the relation>",
                        }
                    ],
                },
                "location": {
                    "id": "43f51215-a1fc-4c60-b6dd-e4afb6d7b831",
                    "name": "Pilestraede 58",
                    "urlFormattedName": "Pilestraede58",
                },
                "countsTowardsTrustScore": False,
                "countsTowardsLocationTrustScore": False,
                "links": [
                    {
                        "href": "<Url for the resource>",
                        "method": "<Http method for the resource>",
                        "rel": "<Description of the relation>",
                    }
                ],
                "reportData": {
                    "source": "Trustpilot",
                    "publicComment": "This review contains sensitive information.",
                    "createdAt": "2013-09-07T13:37:00",
                    "reasons": ["sensitiveInformation", "consumerIsCompetitor"],
                    "reason": "consumer_is_competitor",
                    "reviewVisibility": "hidden",
                },
                "complianceLabels": [None],
                "invitation": {"businessUnitId": "507f191e810c19729de860ea"},
                "businessUnitHistory": [
                    {
                        "businessUnitId": "507f191e810c19729de860ea",
                        "identifyingName": "example.com",
                        "displayName": "Example Inc.",
                        "changeDate": "2013-09-07T13:37:00",
                    }
                ],
                "reviewVerificationLevel": None,
            }
        ],
    }

    with patch("dlt.sources.helpers.requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = sample_response
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        dest_table = "trustpilot.reviews"
        source_uri = "trustpilot://<business_unit_id>?api_key=<api_key>"
        source_table = "reviews"

        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
        )

        assert result.exit_code == 0

        rows = get_query_result(dest_uri, f"SELECT * FROM {dest_table}")
        assert len(rows) > 0, "No data ingested into the destination"


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_trustpilot(dest):
    trustpilot_test_case(dest.start())
    dest.stop()
