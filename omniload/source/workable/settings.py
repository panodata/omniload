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

"""Workable source settings and constants"""

# define which endpoints to load
DEFAULT_ENDPOINTS = (
    "members",
    "recruiters",
    "stages",
    "requisitions",
    "jobs",
    "custom_attributes",
    "events",
)

# define which sub endpoints to load for each main endpoint if details
# are requested
DEFAULT_DETAILS = {
    "candidates": (
        "activities",
        "offer",
    ),
    "jobs": (
        "activities",
        "application_form",
        "questions",
        "stages",
        "custom_attributes",
        "members",
        "recruiters",
    ),
}
