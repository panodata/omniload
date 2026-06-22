from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy

from tests.util import invoke_ingest_command
from tests.warehouse.settings import DESTINATIONS


def pinterest_test_case(dest_uri):
    sample_response = {
        "items": [
            {
                "id": "813744226420795884",
                "created_at": "2020-01-01T20:10:40-00:00",
                "link": "https://www.pinterest.com/",
                "title": "string",
                "description": "string",
                "dominant_color": "#6E7874",
                "alt_text": "string",
                "creative_type": "REGULAR",
                "board_id": "string",
                "board_section_id": "string",
                "board_owner": {"username": "string"},
                "is_owner": "false",
                "media": {
                    "media_type": "string",
                    "images": {
                        "150x150": {
                            "width": 150,
                            "height": 150,
                            "url": "https://i.pinimg.com/150x150/0d/f6/f1/0df6f1f0bfe7aaca849c1bbc3607a34b.jpg",
                        },
                        "400x300": {
                            "width": 400,
                            "height": 300,
                            "url": "https://i.pinimg.com/400x300/0d/f6/f1/0df6f1f0bfe7aaca849c1bbc3607a34b.jpg",
                        },
                        "600x": {
                            "width": 600,
                            "height": 600,
                            "url": "https://i.pinimg.com/600x/0d/f6/f1/0df6f1f0bfe7aaca849c1bbc3607a34b.jpg",
                        },
                        "1200x": {
                            "width": 1200,
                            "height": 1200,
                            "url": "https://i.pinimg.com/1200x/0d/f6/f1/0df6f1f0bfe7aaca849c1bbc3607a34b.jpg",
                        },
                    },
                },
                "parent_pin_id": "string",
                "is_standard": "false",
                "has_been_promoted": "false",
                "note": "string",
                "pin_metrics": {
                    "90d": {"pin_click": 7, "impression": 2, "clickthrough": 3},
                    "lifetime_metrics": {
                        "pin_click": 7,
                        "impression": 2,
                        "clickthrough": 3,
                        "reaction": 10,
                        "comment": 2,
                    },
                },
                "is_removable": True,
            }
        ],
        "bookmark": "string",
    }

    sample_response_last = {"items": []}

    with patch("dlt.sources.helpers.requests.Session.get") as mock_get:
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = sample_response
        mock_response_1.raise_for_status = MagicMock()

        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = sample_response_last
        mock_response_2.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_response_1, mock_response_2]
        dest_table = "dest.pins"
        source_uri = "pinterest://?access_token=token_123"
        source_table = "pins"

        result = invoke_ingest_command(
            source_uri,
            source_table,
            dest_uri,
            dest_table,
        )

        assert result.exit_code == 0

        engine = sqlalchemy.create_engine(dest_uri)
        with engine.connect() as conn:
            rows = conn.exec_driver_sql(f"SELECT * FROM {dest_table}").fetchall()
            assert len(rows) > 0, "No data ingested into the destination"
        engine.dispose()


@pytest.mark.parametrize(
    "dest", list(DESTINATIONS.values()), ids=list(DESTINATIONS.keys())
)
def test_pinterest_test_case(dest):
    pinterest_test_case(dest.start())
    dest.stop()
