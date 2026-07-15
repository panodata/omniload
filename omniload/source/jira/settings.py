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

# Define endpoints
DEFAULT_ENDPOINTS = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "use_cursor_pagination": True,
        "params": {
            "fields": "*all",
            "expand": "fields,changelog,operations,transitions,names",
            "validateQuery": "strict",
            "jql": "created >= '2000-01-01' order by created DESC",
        },
    },
    "users": {
        "api_path": "rest/api/3/users",
        "params": {"includeInactiveUsers": True},
    },
    "workflows": {
        "data_path": "values",
        "api_path": "/rest/api/3/workflow/search",
        "params": {},
    },
    "projects": {
        "data_path": "values",
        "api_path": "rest/api/3/project/search",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
    },
}
DEFAULT_PAGE_SIZE = 50
