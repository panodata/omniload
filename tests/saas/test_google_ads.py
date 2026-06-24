# Copyright 2022-2025 ScaleVector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from omniload.source.google_ads.adapter import Report, extract_fields

FIELD_PATHS = [
    "customer.id",
    "campaign.id",
    "campaign.name",
    "ad_group.id",
    "ad_group.name",
    "ad_group_ad.resource_name",
    "ad_group_ad.status",
    "ad_group_ad.ad.id",
    "ad_group_ad.ad.type",
    "ad_group_ad.ad.name",
    "ad_group_ad.ad.final_urls",
    "ad_group_ad.ad.responsive_search_ad.path1",
    "ad_group_ad.ad.responsive_search_ad.path2",
    "ad_group_ad.ad.responsive_display_ad.long_headline",
    "ad_group_ad.ad.responsive_display_ad.call_to_action_text",
    "ad_group_ad.ad.responsive_display_ad.format_setting",
    "ad_group_ad.ad.responsive_display_ad.headlines",
    "ad_group_ad.ad.responsive_display_ad.descriptions",
]

EXPECTED_KEYS = {path.replace(".", "_") for path in FIELD_PATHS}


def test_extract_fields():
    display_ad_data = {
        "customer": {
            "resource_name": "customers/1234567890",
            "id": "1234567890",
        },
        "campaign": {
            "resource_name": "customers/1234567890/campaigns/111",
            "name": "Summer Display Campaign",
            "id": "111",
        },
        "ad_group": {
            "resource_name": "customers/1234567890/adGroups/222",
            "id": "222",
            "name": "Display Ad Group",
        },
        "ad_group_ad": {
            "resource_name": "customers/1234567890/adGroupAds/222~333",
            "status": "ENABLED",
            "ad": {
                "type": "RESPONSIVE_DISPLAY_AD",
                "responsive_display_ad": {
                    "headlines": [{"text": "Buy Now"}],
                    "long_headline": {"text": "Great deals on summer products"},
                    "descriptions": [
                        {"text": "Free shipping on all orders"},
                        {"text": "Limited time offer"},
                    ],
                    "format_setting": "ALL_FORMATS",
                },
                "resource_name": "customers/1234567890/ads/333",
                "id": "333",
                "final_urls": ["https://example.com/summer"],
            },
        },
    }

    call_ad_data = {
        "customer": {
            "resource_name": "customers/1234567890",
            "id": "1234567890",
        },
        "campaign": {
            "resource_name": "customers/1234567890/campaigns/444",
            "name": "Call Campaign",
            "id": "444",
        },
        "ad_group": {
            "resource_name": "customers/1234567890/adGroups/555",
            "id": "555",
            "name": "Call Ad Group",
        },
        "ad_group_ad": {
            "resource_name": "customers/1234567890/adGroupAds/555~666",
            "status": "PAUSED",
            "ad": {
                "type": "CALL_AD",
                "resource_name": "customers/1234567890/ads/666",
                "id": "666",
                "final_urls": ["https://example.com/contact"],
            },
        },
    }

    search_ad_data = {
        "customer": {
            "resource_name": "customers/1234567890",
            "id": "1234567890",
        },
        "campaign": {
            "resource_name": "customers/1234567890/campaigns/777",
            "name": "Search Campaign",
            "id": "777",
        },
        "ad_group": {
            "resource_name": "customers/1234567890/adGroups/888",
            "id": "888",
            "name": "Search Ad Group",
        },
        "ad_group_ad": {
            "resource_name": "customers/1234567890/adGroupAds/888~999",
            "status": "PAUSED",
            "ad": {
                "type": "RESPONSIVE_SEARCH_AD",
                "responsive_search_ad": {
                    "path1": "deals",
                    "path2": "today",
                },
                "resource_name": "customers/1234567890/ads/999",
                "id": "999",
                "final_urls": ["https://example.com/search"],
            },
        },
    }

    for row_data in [display_ad_data, call_ad_data, search_ad_data]:
        result = extract_fields(row_data, FIELD_PATHS)
        assert set(result.keys()) == EXPECTED_KEYS

    # display ad
    display = extract_fields(display_ad_data, FIELD_PATHS)
    assert display["customer_id"] == "1234567890"
    assert display["campaign_id"] == "111"
    assert display["campaign_name"] == "Summer Display Campaign"
    assert display["ad_group_id"] == "222"
    assert display["ad_group_name"] == "Display Ad Group"
    assert (
        display["ad_group_ad_resource_name"]
        == "customers/1234567890/adGroupAds/222~333"
    )
    assert display["ad_group_ad_status"] == "ENABLED"
    assert display["ad_group_ad_ad_id"] == "333"
    assert display["ad_group_ad_ad_type"] == "RESPONSIVE_DISPLAY_AD"
    assert display["ad_group_ad_ad_final_urls"] == ["https://example.com/summer"]
    assert display["ad_group_ad_ad_responsive_display_ad_headlines"] == [
        {"text": "Buy Now"}
    ]
    assert display["ad_group_ad_ad_responsive_display_ad_long_headline"] == {
        "text": "Great deals on summer products"
    }
    assert display["ad_group_ad_ad_responsive_display_ad_descriptions"] == [
        {"text": "Free shipping on all orders"},
        {"text": "Limited time offer"},
    ]
    assert (
        display["ad_group_ad_ad_responsive_display_ad_format_setting"] == "ALL_FORMATS"
    )
    assert display["ad_group_ad_ad_responsive_search_ad_path1"] is None
    assert display["ad_group_ad_ad_responsive_search_ad_path2"] is None
    assert display["ad_group_ad_ad_name"] is None

    # call ad
    call = extract_fields(call_ad_data, FIELD_PATHS)
    assert call["customer_id"] == "1234567890"
    assert call["campaign_name"] == "Call Campaign"
    assert call["ad_group_ad_status"] == "PAUSED"
    assert call["ad_group_ad_ad_type"] == "CALL_AD"
    assert call["ad_group_ad_ad_id"] == "666"
    assert call["ad_group_ad_ad_final_urls"] == ["https://example.com/contact"]
    assert call["ad_group_ad_ad_responsive_display_ad_headlines"] is None
    assert call["ad_group_ad_ad_responsive_display_ad_descriptions"] is None
    assert call["ad_group_ad_ad_responsive_display_ad_long_headline"] is None
    assert call["ad_group_ad_ad_responsive_search_ad_path1"] is None
    assert call["ad_group_ad_ad_responsive_search_ad_path2"] is None
    assert call["ad_group_ad_ad_name"] is None

    # search ad
    search = extract_fields(search_ad_data, FIELD_PATHS)
    assert search["customer_id"] == "1234567890"
    assert search["campaign_name"] == "Search Campaign"
    assert search["ad_group_ad_ad_type"] == "RESPONSIVE_SEARCH_AD"
    assert search["ad_group_ad_ad_responsive_search_ad_path1"] == "deals"
    assert search["ad_group_ad_ad_responsive_search_ad_path2"] == "today"
    assert search["ad_group_ad_ad_final_urls"] == ["https://example.com/search"]
    assert search["ad_group_ad_ad_responsive_display_ad_headlines"] is None
    assert search["ad_group_ad_ad_responsive_display_ad_descriptions"] is None
    assert search["ad_group_ad_ad_responsive_display_ad_long_headline"] is None
    assert search["ad_group_ad_ad_name"] is None


class TestReportPrimaryKeys(unittest.TestCase):
    def test_empty_dimensions_and_segments(self):
        """Primary keys should return empty list when no dimensions or segments."""
        report = Report(
            resource="campaign",
            dimensions=[],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, [])

    def test_dimensions_only_no_id_or_name(self):
        """Dimensions without .id or .name should be included and converted."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.status", "customer.currency_code"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["campaign_status", "customer_currency_code"])

    def test_dimensions_with_id_fields(self):
        """Dimensions with .id fields should be included and converted."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.id", "customer.id"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["campaign_id", "customer_id"])

    def test_includes_name_and_id_fields(self):
        """Both .id and .name fields should be included."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.id", "campaign.name", "customer.id"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["campaign_id", "campaign_name", "customer_id"])

    def test_keeps_name_when_no_matching_id(self):
        """Name fields should be kept when no matching .id exists."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.name", "customer.id"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        # campaign.name should be kept because campaign.id doesn't exist
        self.assertEqual(result, ["campaign_name", "customer_id"])

    def test_segments_only(self):
        """Segments should be processed the same as dimensions."""
        report = Report(
            resource="campaign",
            dimensions=[],
            metrics=["metrics.clicks"],
            segments=["segments.date", "segments.device"],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["segments_date", "segments_device"])

    def test_dimensions_and_segments_combined(self):
        """Both dimensions and segments should be combined in primary keys."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.id", "customer.id"],
            metrics=["metrics.clicks"],
            segments=["segments.date", "segments.ad_network_type"],
        )
        result = report.primary_keys()
        self.assertEqual(
            result,
            ["campaign_id", "customer_id", "segments_date", "segments_ad_network_type"],
        )

    def test_name_across_dimensions_and_segments(self):
        """All fields from both dimensions and segments should be included."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.id", "campaign.name"],
            metrics=["metrics.clicks"],
            segments=["customer.id", "customer.name"],
        )
        result = report.primary_keys()
        self.assertEqual(
            result, ["campaign_id", "campaign_name", "customer_id", "customer_name"]
        )

    def test_multiple_name_fields_with_single_id(self):
        """All fields including multiple .name fields should be included."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.id", "campaign.name", "ad_group.name"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["campaign_id", "campaign_name", "ad_group_name"])

    def test_nested_field_id_and_name(self):
        """Nested fields like ad_group_ad.ad.id should be included."""
        report = Report(
            resource="ad_group_ad",
            dimensions=["ad_group_ad.ad.id", "ad_group_ad.ad.name"],
            metrics=["metrics.clicks"],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["ad_group_ad_ad_id", "ad_group_ad_ad_name"])

    def test_preserves_order(self):
        """Primary keys should preserve the order of dimensions then segments."""
        report = Report(
            resource="campaign",
            dimensions=["customer.id", "campaign.id", "ad_group.id"],
            metrics=["metrics.clicks"],
            segments=["segments.date", "segments.device"],
        )
        result = report.primary_keys()
        self.assertEqual(
            result,
            [
                "customer_id",
                "campaign_id",
                "ad_group_id",
                "segments_date",
                "segments_device",
            ],
        )

    def test_id_in_segment_and_name_in_dimension(self):
        """Both .name in dimensions and .id in segments should be included."""
        report = Report(
            resource="campaign",
            dimensions=["campaign.name"],
            metrics=["metrics.clicks"],
            segments=["campaign.id"],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["campaign_name", "campaign_id"])

    def test_real_world_campaign_report(self):
        """Test with a realistic campaign report configuration."""
        report = Report(
            resource="campaign",
            dimensions=[
                "campaign.id",
                "campaign.name",
                "customer.id",
                "customer.descriptive_name",
            ],
            metrics=[
                "metrics.clicks",
                "metrics.impressions",
                "metrics.cost_micros",
            ],
            segments=["segments.date", "segments.ad_network_type", "segments.device"],
        )
        result = report.primary_keys()
        self.assertEqual(
            result,
            [
                "campaign_id",
                "campaign_name",
                "customer_id",
                "customer_descriptive_name",
                "segments_date",
                "segments_ad_network_type",
                "segments_device",
            ],
        )

    def test_field_to_column_conversion(self):
        """Verify that dots are converted to underscores in column names."""
        report = Report(
            resource="test",
            dimensions=["a.b.c.d"],
            metrics=[],
            segments=[],
        )
        result = report.primary_keys()
        self.assertEqual(result, ["a_b_c_d"])


if __name__ == "__main__":
    unittest.main()
