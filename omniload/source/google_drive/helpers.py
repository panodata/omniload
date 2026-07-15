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

import io
from typing import Any

from googleapiclient.errors import HttpError  # type: ignore
from googleapiclient.http import MediaIoBaseDownload  # type: ignore


def download_file_from_google_drive(service: Any, file_id: str, file_path: str) -> None:
    try:
        # Create a request to download the file
        request = service.files().get_media(fileId=file_id)
        # Create a BytesIO object to store the file content
        fh = io.BytesIO()
        # Create a downloader object to handle the download
        downloader = MediaIoBaseDownload(fh, request)
        # Flag to track if the download is complete
        done = False
        # Download the next chunk of data and check if the download is complete
        while not done:
            _, done = downloader.next_chunk()
        # Write the downloaded file content to the specified file path
        with open(file_path, "wb") as f:
            f.write(fh.getvalue())

    except HttpError as error:
        print(f"An error occurred: {error}")
