from urllib.parse import parse_qs, urlparse


class DoceboSource:
    def handles_incrementality(self) -> bool:
        return False

    def dlt_source(self, uri: str, table: str, **kwargs):
        # docebo://?base_url=https://yourcompany.docebosaas.com&client_id=xxx&client_secret=xxx
        # Optional: &username=xxx&password=xxx for password grant type

        if kwargs.get("incremental_key"):
            raise ValueError("Incremental loads are not yet supported for Docebo")

        parsed_uri = urlparse(uri)
        source_params = parse_qs(parsed_uri.query)

        base_url = source_params.get("base_url")
        if not base_url:
            raise ValueError("base_url is required to connect to Docebo")

        client_id = source_params.get("client_id")
        if not client_id:
            raise ValueError("client_id is required to connect to Docebo")

        client_secret = source_params.get("client_secret")
        if not client_secret:
            raise ValueError("client_secret is required to connect to Docebo")

        # Username and password are optional (uses client_credentials grant if not provided)
        username = source_params.get("username", [None])[0]
        password = source_params.get("password", [None])[0]

        # Supported tables
        supported_tables = [
            "users",
            "courses",
            "user_fields",
            "branches",
            "groups",
            "group_members",
            "course_fields",
            "learning_objects",
            "learning_plans",
            "learning_plan_enrollments",
            "learning_plan_course_enrollments",
            "course_enrollments",
            "sessions",
            "categories",
            "certifications",
            "external_training",
            "survey_answers",
        ]
        if table not in supported_tables:
            raise ValueError(
                f"Resource '{table}' is not supported for Docebo source. Supported tables: {', '.join(supported_tables)}"
            )

        from omniload.source.docebo.adapter import docebo_source

        return docebo_source(
            base_url=base_url[0],
            client_id=client_id[0],
            client_secret=client_secret[0],
            username=username,
            password=password,
        ).with_resources(table)
