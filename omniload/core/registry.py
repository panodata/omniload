from typing import Dict, Type

from omniload.core.model import DestinationProtocol, SourceProtocol
from omniload.source.adjust.api import AdjustSource
from omniload.source.airtable.api import AirtableSource
from omniload.source.allium.api import AlliumSource
from omniload.source.anthropic.api import AnthropicSource
from omniload.source.applovin.api import AppLovinSource
from omniload.source.applovin_max.api import ApplovinMaxSource
from omniload.source.appsflyer.api import AppsflyerSource
from omniload.source.appstore.api import AppleAppStoreSource
from omniload.source.arrow.api import ArrowMemoryMappedSource
from omniload.source.asana.api import AsanaSource
from omniload.source.attio.api import AttioSource
from omniload.source.blobstorage.api import GCSSource, S3Source
from omniload.source.bruin.api import BruinSource
from omniload.source.chess.api import ChessSource
from omniload.source.clickup.api import ClickupSource
from omniload.source.couchbase.api import CouchbaseSource
from omniload.source.csv.api import LocalCsvSource
from omniload.source.cursor.api import CursorSource
from omniload.source.customer_io.api import CustomerIoSource
from omniload.source.docebo.api import DoceboSource
from omniload.source.dune.api import DuneSource
from omniload.source.dynamodb.api import DynamoDBSource
from omniload.source.elasticsearch.api import ElasticsearchSource
from omniload.source.facebook_ads.api import FacebookAdsSource
from omniload.source.fireflies.api import FirefliesSource
from omniload.source.fluxx.api import FluxxSource
from omniload.source.frankfurter.api import FrankfurterSource
from omniload.source.freshdesk.api import FreshdeskSource
from omniload.source.fundraiseup.api import FundraiseupSource
from omniload.source.github.api import GitHubSource
from omniload.source.google_ads.api import GoogleAdsSource
from omniload.source.google_analytics.api import GoogleAnalyticsSource
from omniload.source.google_sheets.api import GoogleSheetsSource
from omniload.source.gorgias.api import GorgiasSource
from omniload.source.hostaway.api import HostawaySource
from omniload.source.http.api import HttpSource
from omniload.source.hubspot.api import HubspotSource
from omniload.source.indeed.api import IndeedSource
from omniload.source.influxdb.api import InfluxDBSource
from omniload.source.intercom.api import IntercomSource
from omniload.source.isoc_pulse.api import IsocPulseSource
from omniload.source.jira.api import JiraSource
from omniload.source.kafka.api import KafkaSource
from omniload.source.kinesis.api import KinesisSource
from omniload.source.klaviyo.api import KlaviyoSource
from omniload.source.linear.api import LinearSource
from omniload.source.linkedin_ads.api import LinkedInAdsSource
from omniload.source.mailchimp.api import MailchimpSource
from omniload.source.mixpanel.api import MixpanelSource
from omniload.source.monday.api import MondaySource
from omniload.source.mongodb.api import MongoDbSource
from omniload.source.notion.api import NotionSource
from omniload.source.personio.api import PersonioSource
from omniload.source.phantombuster.api import PhantombusterSource
from omniload.source.pinterest.api import PinterestSource
from omniload.source.pipedrive.api import PipedriveSource
from omniload.source.plusvibeai.api import PlusVibeAISource
from omniload.source.primer.api import PrimerSource
from omniload.source.quickbooks.api import QuickBooksSource
from omniload.source.reddit_ads.api import RedditAdsSource
from omniload.source.revenuecat.api import RevenueCatSource
from omniload.source.salesforce.api import SalesforceSource
from omniload.source.sftp.api import SFTPSource
from omniload.source.shopify.api import ShopifySource
from omniload.source.slack.api import SlackSource
from omniload.source.smartsheets.api import SmartsheetSource
from omniload.source.snapchat_ads.api import SnapchatAdsSource
from omniload.source.socrata.api import SocrataSource
from omniload.source.solidgate.api import SolidgateSource
from omniload.source.stripe.api import StripeAnalyticsSource
from omniload.source.tiktok_ads.api import TikTokSource
from omniload.source.trustpilot.api import TrustpilotSource
from omniload.source.wise.api import WiseSource
from omniload.source.zendesk.api import ZendeskSource
from omniload.source.zoom.api import ZoomSource
from omniload.target.athena import AthenaDestination
from omniload.target.bigquery import BigQueryDestination
from omniload.target.blobstorage import GCSDestination, S3Destination
from omniload.target.clickhouse import ClickhouseDestination
from omniload.target.cratedb import CrateDBDestination
from omniload.target.csv import CsvDestination
from omniload.target.databricks import DatabricksDestination
from omniload.target.duckdb import DuckDBDestination
from omniload.target.elasticsearch.api import ElasticsearchDestination
from omniload.target.mongodb import MongoDBDestination
from omniload.target.motherduck import MotherduckDestination
from omniload.target.mssql import MsSQLDestination
from omniload.target.mysql import MySqlDestination
from omniload.target.postgresql import PostgresDestination
from omniload.target.redshift import RedshiftDestination
from omniload.target.snowflake import SnowflakeDestination
from omniload.target.sqlite import SqliteDestination
from omniload.target.synapse import SynapseDestination
from omniload.target.trino import TrinoDestination

SQL_SOURCE_SCHEMES = [
    "bigquery",
    "crate",
    "cratedb",
    "duckdb",
    "mssql",
    "mssql+pyodbc",
    "mysql",
    "mysql+pymysql",
    "mysql+mysqlconnector",
    "md",
    "motherduck",
    "postgres",
    "postgresql",
    "postgresql+psycopg2",
    "redshift",
    "redshift+psycopg2",
    "snowflake",
    "sqlite",
    "oracle",
    "oracle+cx_oracle",
    "oracle+oracledb",
    "hana",
    "clickhouse",
    "databricks",
    "db2",
    "spanner",
    "trino",
]


sources: Dict[str, Type[SourceProtocol]] = {
    "allium": AlliumSource,
    "anthropic": AnthropicSource,
    "bruin": BruinSource,
    "csv": LocalCsvSource,
    "couchbase": CouchbaseSource,
    "cursor": CursorSource,
    "docebo": DoceboSource,
    "dune": DuneSource,
    "http": HttpSource,
    "https": HttpSource,
    "mongodb": MongoDbSource,
    "mongodb+srv": MongoDbSource,
    "notion": NotionSource,
    "gsheets": GoogleSheetsSource,
    "shopify": ShopifySource,
    "gorgias": GorgiasSource,
    "github": GitHubSource,
    "chess": ChessSource,
    "stripe": StripeAnalyticsSource,
    "facebookads": FacebookAdsSource,
    "fluxx": FluxxSource,
    "slack": SlackSource,
    "hostaway": HostawaySource,
    "hubspot": HubspotSource,
    "indeed": IndeedSource,
    "intercom": IntercomSource,
    "jira": JiraSource,
    "airtable": AirtableSource,
    "klaviyo": KlaviyoSource,
    "mixpanel": MixpanelSource,
    "appsflyer": AppsflyerSource,
    "kafka": KafkaSource,
    "adjust": AdjustSource,
    "zendesk": ZendeskSource,
    "mmap": ArrowMemoryMappedSource,
    "s3": S3Source,
    "dynamodb": DynamoDBSource,
    "asana": AsanaSource,
    "tiktok": TikTokSource,
    "googleanalytics": GoogleAnalyticsSource,
    "googleads": GoogleAdsSource,
    "appstore": AppleAppStoreSource,
    "gs": GCSSource,
    "linkedinads": LinkedInAdsSource,
    "linear": LinearSource,
    "applovin": AppLovinSource,
    "applovinmax": ApplovinMaxSource,
    "salesforce": SalesforceSource,
    "personio": PersonioSource,
    "kinesis": KinesisSource,
    "pipedrive": PipedriveSource,
    "frankfurter": FrankfurterSource,
    "freshdesk": FreshdeskSource,
    "fundraiseup": FundraiseupSource,
    "trustpilot": TrustpilotSource,
    "phantombuster": PhantombusterSource,
    "elasticsearch": ElasticsearchSource,
    "attio": AttioSource,
    "solidgate": SolidgateSource,
    "quickbooks": QuickBooksSource,
    "isoc-pulse": IsocPulseSource,
    "smartsheet": SmartsheetSource,
    "sftp": SFTPSource,
    "pinterest": PinterestSource,
    "redditads": RedditAdsSource,
    "revenuecat": RevenueCatSource,
    "socrata": SocrataSource,
    "snapchatads": SnapchatAdsSource,
    "zoom": ZoomSource,
    "clickup": ClickupSource,
    "influxdb": InfluxDBSource,
    "wise": WiseSource,
    "plusvibeai": PlusVibeAISource,
    "monday": MondaySource,
    "mailchimp": MailchimpSource,
    "primer": PrimerSource,
    "fireflies": FirefliesSource,
    "customerio": CustomerIoSource,
}
destinations: Dict[str, Type[DestinationProtocol]] = {
    "bigquery": BigQueryDestination,
    "cratedb": CrateDBDestination,
    "databricks": DatabricksDestination,
    "duckdb": DuckDBDestination,
    "motherduck": MotherduckDestination,
    "md": MotherduckDestination,
    "mssql": MsSQLDestination,
    "postgres": PostgresDestination,
    "postgresql": PostgresDestination,
    "postgresql+psycopg2": PostgresDestination,
    "redshift": RedshiftDestination,
    "redshift+psycopg2": RedshiftDestination,
    "redshift+redshift_connector": RedshiftDestination,
    "snowflake": SnowflakeDestination,
    "synapse": SynapseDestination,
    "csv": CsvDestination,
    "athena": AthenaDestination,
    "clickhouse+native": ClickhouseDestination,
    "clickhouse": ClickhouseDestination,
    "elasticsearch": ElasticsearchDestination,
    "mongodb": MongoDBDestination,
    "mongodb+srv": MongoDBDestination,
    "s3": S3Destination,
    "gs": GCSDestination,
    "sqlite": SqliteDestination,
    "mysql": MySqlDestination,
    "mysql+pymysql": MySqlDestination,
    "trino": TrinoDestination,
}
