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

import typing as t
from queue import Empty, Queue

from dlt.common import logger


# Please read more at https://mypy.readthedocs.io/en/stable/runtime_troubles.html#not-generic-runtime
T = t.TypeVar("T")

if t.TYPE_CHECKING:

    class _Queue(Queue[T]):
        pass

else:

    class _Queue(Queue, t.Generic[T]):
        pass


class QueueClosedError(Exception):
    pass


class ScrapingQueue(_Queue[T]):
    def __init__(
        self,
        maxsize: int = 0,
        batch_size: int = 10,
        read_timeout: float = 1.0,
    ) -> None:
        super().__init__(maxsize)
        self.batch_size = batch_size
        self.read_timeout = read_timeout
        self._is_closed = False

    def get_batches(self) -> t.Iterator[t.Any]:
        """Batching helper can be wrapped as a dlt.resource

        Returns:
            Iterator[Any]: yields scraped items one by one
        """
        batch: t.List[T] = []
        while True:
            if len(batch) == self.batch_size:
                yield batch
                batch = []

            try:
                if self.is_closed:
                    try:
                        item = self.get_nowait()
                    except Empty:
                        raise QueueClosedError("Queue is closed")
                else:
                    item = self.get(timeout=self.read_timeout)
                batch.append(item)

                # Mark task as completed
                self.task_done()
            except Empty:
                if batch:
                    yield batch
                    batch = []
            except QueueClosedError:
                logger.info("Queue is closed and drained")

                if batch:
                    yield batch

                break

    def stream(self) -> t.Iterator[t.Any]:
        """Streaming generator, wraps get_batches
        and handles `GeneratorExit` if dlt closes it.

        Returns:
            t.Iterator[t.Any]: returns batches of scraped content
        """
        try:
            yield from self.get_batches()
        except GeneratorExit:
            self.close()

    def close(self) -> None:
        """Marks queue as closed"""
        self._is_closed = True

    @property
    def is_closed(self) -> bool:
        return self._is_closed
