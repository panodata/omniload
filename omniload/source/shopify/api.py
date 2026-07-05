from urllib.parse import parse_qs, urlparse


class ShopifySource:
    def handles_incrementality(self) -> bool:
        return True

    def _get_shopify_access_token(
        self, shop_url: str, client_id: str, client_secret: str
    ) -> str:
        import requests

        token_url = f"{shop_url}/admin/oauth/access_token"

        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        response = requests.post(
            token_url,
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )

        if response.status_code != 200:
            raise ValueError(
                f"Failed to get Shopify access token: {response.status_code} - {response.text}"
            )

        token_data = response.json()
        return token_data.get("access_token")

    def dlt_source(self, uri: str, table: str, **kwargs):
        if kwargs.get("incremental_key"):
            raise ValueError(
                "Shopify takes care of incrementality on its own, you should not provide incremental_key"
            )

        source_fields = urlparse(uri)
        source_params = parse_qs(source_fields.query)
        api_key = source_params.get("api_key")

        if not api_key:
            client_id = source_params.get("client_id")
            client_secret = source_params.get("client_secret")

            if not client_id or not client_secret:
                raise ValueError(
                    "Either api_key or both client_id and client_secret must be provided in the URI"
                )

            shop_url = f"https://{source_fields.netloc}"
            access_token = self._get_shopify_access_token(
                shop_url, client_id[0], client_secret[0]
            )
            api_key = [access_token]

        date_args = {}
        if kwargs.get("interval_start"):
            date_args["start_date"] = kwargs.get("interval_start")

        if kwargs.get("interval_end"):
            date_args["end_date"] = kwargs.get("interval_end")

        resource = None
        if table in [
            "products",
            "products_legacy",
            "orders",
            "customers",
            "inventory_items",
            "transactions",
            "balance",
            "events",
            "price_rules",
            "discounts",
            "taxonomy",
        ]:
            resource = table
        else:
            raise ValueError(
                f"Table name '{table}' is not supported for Shopify source yet, if you are interested in it please create a GitHub issue at https://github.com/panodata/omniload"
            )

        from omniload.source.shopify.adapter import shopify_source

        return shopify_source(
            private_app_password=api_key[0],
            shop_url=f"https://{source_fields.netloc}",
            **date_args,  # ty: ignore[invalid-argument-type]
        ).with_resources(resource)
