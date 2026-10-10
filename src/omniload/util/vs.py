import logging
import shutil
from contextlib import chdir
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import platformdirs
from git import Repo

from omniload.util.log import setup_logging

logger = logging.getLogger(__name__)


FILE_HEADER = b"""
# Copyright 2022-2026 ScaleVector
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

""".lstrip()


@dataclass
class Rule:
    source: str
    target: Optional[str]
    category: Optional[str] = None
    use_tests: Optional[bool] = True


class VerifiedSourcesRecipe(list):
    url: str = "https://github.com/dlt-hub/verified-sources.git"
    code_path: str = "sources"
    test_path: str = "tests"

    def __init__(self):
        super().__init__()
        self.append(Rule(source="airtable", target="airtable", category="saas"))
        self.append(Rule(source="asana_dlt", target="asana", category="saas"))
        self.append(
            Rule(source="bing_webmaster", target="bing_webmaster", category="saas")
        )
        self.append(Rule(source="chess", target="chess", category="saas"))
        self.append(Rule(source="facebook_ads", target="facebook_ads", category="saas"))
        self.append(Rule(source="filesystem", target=None))
        self.append(Rule(source="freshdesk", target="freshdesk", category="saas"))
        self.append(Rule(source="github", target="github", category="saas"))
        self.append(Rule(source="google_ads", target="google_ads", category="saas"))
        self.append(
            Rule(source="google_analytics", target="google_analytics", category="saas")
        )
        self.append(
            Rule(source="google_sheets", target="google_sheets", category="saas")
        )
        self.append(Rule(source="hubspot", target="hubspot", category="saas"))
        self.append(Rule(source="inbox", target="imap", category="protocol"))
        self.append(Rule(source="jira", target="jira", category="saas"))
        self.append(Rule(source="kafka", target="kafka", category="stream"))
        self.append(Rule(source="kinesis", target="kinesis", category="stream"))
        self.append(Rule(source="matomo", target="matomo", category="saas"))
        self.append(Rule(source="mongodb", target="mongodb", category="database"))
        self.append(Rule(source="mux", target="mux", category="saas"))
        self.append(Rule(source="notion", target="notion", category="saas"))
        self.append(Rule(source="personio", target="personio", category="saas"))
        self.append(
            Rule(source="pg_replication", target="pg_replication", category="database")
        )
        self.append(Rule(source="pipedrive", target="pipedrive", category="saas"))
        self.append(Rule(source="pokemon", target="pokemon", category="saas"))
        self.append(Rule(source="rest_api", target=None))
        self.append(Rule(source="salesforce", target="salesforce", category="saas"))
        self.append(Rule(source="scraping", target="scrapy", category="tool"))
        self.append(Rule(source="shopify_dlt", target="shopify", category="saas"))
        self.append(Rule(source="slack", target="slack", category="saas"))
        self.append(Rule(source="sql_database", target=None))
        self.append(Rule(source="strapi", target="strapi", category="saas"))
        self.append(Rule(source="stripe_analytics", target="stripe", category="saas"))
        self.append(
            Rule(source="unstructured_data", target="unstructured", category="tool")
        )
        self.append(
            Rule(
                source="unstructured_data/google_drive",
                target="google_drive",
                category="storage",
                use_tests=False,
            )
        )
        self.append(Rule(source="workable", target="workable", category="saas"))
        self.append(Rule(source="zendesk", target="zendesk", category="saas"))


class VerifiedSourcesSync:
    def __init__(self, target_path):
        self.target_path = target_path
        self.workdir = platformdirs.user_cache_path("omniload") / "dlt-verified-sources"
        self.workdir.mkdir(parents=True, exist_ok=True)
        logger.info("Working directory: {}".format(self.workdir))
        self.repo = Repo(self.workdir)
        self.tree = self.repo.head.commit.tree
        self.recipe = VerifiedSourcesRecipe()

    def run(self):
        self.acquire_sources()
        self.process()
        self.report_unmapped_modules()

    def process(self):
        """Process and apply recipe rules, copying and rewriting the whole tree"""
        self.copy_code()
        # self.copy_tests()

    def copy_code(self):
        """Copy code from `verified-sources` into omniload"""

        omniload_source_path = self.target_path / "omniload" / "source"
        logger.info(f"Copy code to {omniload_source_path}")

        for rule in self.effective_rules:
            module_path = self.tree / self.recipe.code_path / rule.source

            # Process Python files only.
            # TODO: How to use the README.md files?
            def tree_filter(item, _):
                if item.name.endswith(".py"):
                    return True
                return False

            selected_files = list(module_path.traverse(depth=1, predicate=tree_filter))
            selected_file_names = [item.name for item in selected_files]
            logger.debug(
                'Upstream files in module "%s": %s',
                module_path.name,
                selected_file_names,
            )

            for blob in selected_files:
                payload = blob.data_stream.read()
                file_name = blob.name
                source_path = blob.path

                # Rule: Rename `__init__.py` to `adapter.py`.
                target_file_name = file_name.replace("__init__.py", "adapter.py")

                # Rule: Rename `*_client.py` to `client.py`.
                if file_name.endswith("_client.py"):
                    target_file_name = "client.py"

                # Rule: Skip `setup_script_gcp_oauth.py`.
                if file_name.startswith("setup_script_"):
                    continue

                target_path = omniload_source_path / rule.target / target_file_name
                # logger.debug("%s -> %s", source_path, target_path)
                Path(target_path).parent.mkdir(parents=True, exist_ok=True)
                Path(target_path).write_bytes(FILE_HEADER + payload)

    def copy_tests(self):
        """Copy tests from `verified-sources` into omniload"""

        omniload_tests_path = self.target_path / "tests" / "dlt"
        logger.info(f"Copy tests to {omniload_tests_path}")

        for rule in self.effective_rules:
            if not rule.use_tests:
                continue

            module_path = self.tree / self.recipe.test_path / rule.source

            def tree_filter(item, _):
                if item.name == "__init__.py":
                    return False
                if item.name.endswith(".py"):
                    return True
                return False

            selected_files = list(module_path.traverse(depth=1, predicate=tree_filter))
            selected_file_names = [item.name for item in selected_files]
            logger.debug(
                'Upstream files in module "%s": %s',
                module_path.name,
                selected_file_names,
            )

            use_directory = False
            if len(selected_files) > 1:
                use_directory = True

            for blob in selected_files:
                payload = blob.data_stream.read()
                file_name = blob.name
                source_path = blob.path

                if use_directory:
                    target_path = omniload_tests_path / rule.target / file_name
                else:
                    # Rule: For single-file tests, rename `test_*_{source}.py` to `test_{source}.py`.
                    if file_name.startswith("test_"):
                        file_name = f"test_{rule.target}.py"
                    target_path = omniload_tests_path / file_name

                # logger.debug("%s -> %s", source_path, target_path)
                Path(target_path).parent.mkdir(parents=True, exist_ok=True)
                Path(target_path).write_bytes(FILE_HEADER + payload)

    @property
    def skipped_modules(self):
        return [
            "airtable",
            "asana",
            "chess",
            "kafka",
            "stripe",
        ]

    @property
    def effective_rules(self):
        """Effective recipe rules to apply"""
        rules = []
        for rule in self.recipe:
            if rule.target is None:
                logger.info(f"Skipping module {rule.source}")
                continue
            rules.append(rule)
        return rules

    def acquire_sources(self):
        """Acquire source code repository"""
        if len(list(self.workdir.iterdir())) == 0:
            with chdir(self.workdir):
                Repo.clone_from(self.recipe.url, self.workdir)

    def report_unmapped_modules(self):
        """Inform the user about new modules"""
        modules = self.unmapped_module_names
        if modules:
            logger.warning("Those modules are not mapped yet: %s", modules)
        else:
            logger.info("All existing modules are properly mapped")

    @property
    def unmapped_module_names(self):
        """Module names not mapped by rules"""
        return sorted(set(self.upstream_module_names) - set(self.mapped_module_names))

    @property
    def mapped_module_names(self):
        """Module names covered by recipe rules"""
        return [rule.source for rule in self.recipe]

    @property
    def upstream_module_names(self):
        """Module names in the verified-sources repository"""
        names = []
        for item in self.tree / self.recipe.code_path:
            # Only process directories, skip files and other items.
            if item.type != "tree":
                continue
            # Skip special directories.
            if item.name in [".dlt"]:
                continue
            names.append(item.name)
        return names


if __name__ == "__main__":
    """
    uv pip install gitpython platformdirs
    python -m omniload.util.vs
    """
    setup_logging(debug=True)
    target_path = Path.cwd()
    engine = VerifiedSourcesSync(target_path=target_path)
    engine.run()
