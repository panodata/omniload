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


class VerifiedSourcesRecipe(list):
    url: str = "https://github.com/dlt-hub/verified-sources.git"
    code_path: str = "sources"
    test_path: str = "tests"

    def __init__(self):
        super().__init__()
        self.append(Rule(source="airtable", target="airtable"))
        self.append(Rule(source="asana_dlt", target="asana"))
        self.append(Rule(source="bing_webmaster", target="bing_webmaster"))
        self.append(Rule(source="chess", target="chess"))
        self.append(Rule(source="facebook_ads", target="facebook_ads"))
        self.append(Rule(source="filesystem", target=None))
        self.append(Rule(source="freshdesk", target="freshdesk"))
        self.append(Rule(source="github", target="github"))
        self.append(Rule(source="google_ads", target="google_ads"))
        self.append(Rule(source="google_analytics", target="google_analytics"))
        self.append(Rule(source="google_sheets", target="google_sheets"))
        self.append(Rule(source="hubspot", target="hubspot"))
        self.append(Rule(source="inbox", target="imap"))
        self.append(Rule(source="jira", target="jira"))
        self.append(Rule(source="kafka", target="kafka"))
        self.append(Rule(source="kinesis", target="kinesis"))
        self.append(Rule(source="matomo", target="matomo"))
        self.append(Rule(source="mongodb", target="mongodb"))
        self.append(Rule(source="mux", target="mux"))
        self.append(Rule(source="notion", target="notion"))
        self.append(Rule(source="personio", target="personio"))
        self.append(Rule(source="pg_replication", target="pg_replication"))
        self.append(Rule(source="pipedrive", target="pipedrive"))
        self.append(Rule(source="pokemon", target="pokemon"))
        self.append(Rule(source="rest_api", target=None))
        self.append(Rule(source="salesforce", target="salesforce"))
        self.append(Rule(source="scraping", target="scrapy"))
        self.append(Rule(source="shopify_dlt", target="shopify"))
        self.append(Rule(source="slack", target="slack"))
        self.append(Rule(source="sql_database", target=None))
        self.append(Rule(source="strapi", target="strapi"))
        self.append(Rule(source="stripe_analytics", target="stripe"))
        self.append(Rule(source="unstructured_data", target="unstructured"))
        self.append(
            Rule(source="unstructured_data/google_drive", target="google_drive")
        )
        self.append(Rule(source="workable", target="workable"))
        self.append(Rule(source="zendesk", target="zendesk"))


class VerifiedSourcesSync:
    def __init__(self):
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
        cwd = Path.cwd()
        omniload_source_path = cwd / "omniload" / "source"
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
                logger.debug("Copy from/to: %s -> %s", source_path, target_path)
                Path(target_path).parent.mkdir(parents=True, exist_ok=True)
                Path(target_path).write_bytes(FILE_HEADER + payload)

    @property
    def skipped_modules(self):
        return [
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
    engine = VerifiedSourcesSync()
    engine.run()
