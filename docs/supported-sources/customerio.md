# Customer\.io

[Customer.io](https://customer.io/) is a customer engagement platform that enables businesses to send automated messages across email, push, SMS, and more.

omniload supports Customer.io as a source.

## URI format

The URI format for Customer.io is as follows:

```text
customerio://?api_key=<api-key>&region=<region>
```

URI parameters:

- `api_key`: The API key for authentication with the Customer.io API.
- `region`: The region of your Customer.io account. Must be either `us` (default) or `eu`.

The URI is used to connect to the Customer.io API for extracting data.

## Setting up a Customer.io Integration

To get your API key:

1. Log in to your Customer.io account
2. Go to **Account Settings** > **API Credentials**
3. Create a new **App API Key** with read permissions

Once you have your API key, here's a sample command that will copy the data from Customer.io into a DuckDB database:

```sh
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'broadcasts' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.broadcasts'
```

The result of this command will be a table in the `customerio.duckdb` database.

## Tables

Customer.io source allows ingesting the following sources into separate tables:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| [activities](https://docs.customer.io/integrations/api/app/#tag/activities/listActivities) | id | – | replace | Retrieves account activity log. |
| [broadcasts](https://docs.customer.io/integrations/api/app/#tag/broadcasts/listBroadcasts) | id | updated | merge | Retrieves broadcast campaigns. |
| [broadcast_actions](https://docs.customer.io/integrations/api/app/#operation/listBroadcastActions) | id | updated | merge | Retrieves actions for broadcasts. |
| broadcast_action_metrics:period | broadcast_id, action_id, period, step_index | – | replace | Retrieves metrics for broadcast actions. Period: `hours`, `days`, `weeks`, `months`. |
| [broadcast_messages](https://docs.customer.io/integrations/api/app/#tag/broadcasts/listBroadcasts) | id | – | merge | Retrieves messages sent by broadcasts. |
| broadcast_metrics:period | broadcast_id, period, step_index | – | replace | Retrieves metrics for all broadcasts. Period: `hours`, `days`, `weeks`, `months`. |
| [campaigns](https://docs.customer.io/integrations/api/app/#tag/campaigns/listCampaigns) | id | updated | merge | Retrieves triggered campaigns. |
| [campaign_actions](https://docs.customer.io/integrations/api/app/#tag/campaigns/listCampaignActions) | id | updated | merge | Retrieves actions for campaigns. |
| campaign_action_metrics:period | campaign_id, action_id, period, step_index | – | replace | Retrieves metrics for campaign actions. Period: `hours`, `days`, `weeks`, `months`. |
| [campaign_messages](https://docs.customer.io/integrations/api/app/#tag/campaigns/getCampaignMessages) | id | – | merge | Retrieves messages/deliveries sent from campaigns. |
| campaign_metrics:period | campaign_id, period, step_index | – | replace | Retrieves metrics for all campaigns. Period: `hours`, `days`, `weeks`, `months`. |
| [collections](https://docs.customer.io/integrations/api/app/#tag/collections/getCollections) | id | updated_at | merge | Retrieves data collections. |
| [customers](https://docs.customer.io/integrations/api/app/#tag/customers/getPeopleFilter) | cio_id | – | replace | Retrieves all customers/people in the workspace. |
| [customer_activities](https://docs.customer.io/integrations/api/app/#tag/customers/getPersonActivities) | id | – | replace | Retrieves activities performed by each customer. |
| [customer_attributes](https://docs.customer.io/integrations/api/app/#tag/customers/getPersonAttributes) | customer_id | – | replace | Retrieves attributes for each customer. |
| [customer_messages](https://docs.customer.io/integrations/api/app/#tag/customers/getPersonMessages) | id | – | merge | Retrieves messages sent to each customer. |
| [customer_relationships](https://docs.customer.io/integrations/api/app/#tag/customers/getPersonRelationships) | customer_id, object_type_id, object_id | – | replace | Retrieves object relationships for each customer. |
| [exports](https://docs.customer.io/integrations/api/app/#tag/exports/listExports) | id | updated_at | merge | Retrieves export jobs. |
| [info_ip_addresses](https://docs.customer.io/integrations/api/app/#tag/info/getCioAllowlist) | ip | – | replace | Retrieves IP addresses used by Customer.io. |
| [messages](https://docs.customer.io/integrations/api/app/#tag/messages/listMessages) | id | – | merge | Retrieves sent messages. |
| [newsletters](https://docs.customer.io/integrations/api/app/#tag/newsletters/listNewsletters) | id | updated | merge | Retrieves newsletters. |
| newsletter_metrics:period | newsletter_id, period, step_index | – | replace | Retrieves metrics for all newsletters. Period: `hours`, `days`, `weeks`, `months`. |
| [newsletter_test_groups](https://docs.customer.io/integrations/api/app/#tag/newsletter-variants/getNewsletterTestGroups) | id | – | replace | Retrieves test groups for newsletters. |
| [object_types](https://docs.customer.io/integrations/api/app/#tag/objects/getObjectTypes) | id | – | replace | Retrieves object types in the workspace. |
| [objects](https://docs.customer.io/integrations/api/app/#tag/objects/getObjectsFilter) | object_type_id, object_id | – | replace | Retrieves all objects for each object type. |
| [reporting_webhooks](https://docs.customer.io/integrations/api/app/#tag/reporting-webhooks) | id | – | replace | Retrieves reporting webhooks. |
| [segments](https://docs.customer.io/integrations/api/app/#tag/segments/listSegments) | id | updated_at | merge | Retrieves customer segments. |
| [sender_identities](https://docs.customer.io/integrations/api/app/#tag/sender-identities) | id | – | replace | Retrieves sender identities. |
| [subscription_topics](https://docs.customer.io/integrations/api/app/#tag/subscription-center/getTopics) | id | – | replace | Retrieves subscription topics. |
| [transactional_messages](https://docs.customer.io/integrations/api/app/#tag/transactional/listTransactional) | id | – | replace | Retrieves transactional message templates. |
| [workspaces](https://docs.customer.io/integrations/api/app/#tag/workspaces/listWorkspaces) | id | – | replace | Retrieves workspaces in your account. |

Use these as `--source-table` parameter in the `omniload ingest` command.

## Examples

### Metrics Tables

Metrics tables require a period suffix. Use the format `table_name:period` where period can be `hours`, `days`, `weeks`, or `months`.

```sh
# Get daily broadcast metrics
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'broadcast_metrics:days' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.broadcast_metrics'

# Get hourly campaign metrics
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'campaign_metrics:hours' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.campaign_metrics'
```

### People and Objects

```sh
# Get all customers with their identifiers
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'customers' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.customers'

# Get detailed customer attributes
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'customer_attributes' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.customer_attributes'
```

## Incremental Loading

Customer.io supports incremental loading for tables that have an `updated` or `updated_at` field. When using the `--interval-start` and `--interval-end` flags, omniload will only fetch records that have been updated within the specified time range.

```sh
omniload ingest \
  --source-uri 'customerio://?api_key=your_api_key&region=us' \
  --source-table 'broadcasts' \
  --dest-uri duckdb:///customerio.duckdb \
  --dest-table 'customerio.broadcasts' \
  --interval-start '2024-01-01' \
  --interval-end '2024-01-31'
```
