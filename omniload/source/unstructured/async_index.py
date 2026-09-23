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

from typing import Any, List, Optional

from langchain.base_language import BaseLanguageModel
from langchain.chains.retrieval_qa.base import RetrievalQA
from langchain.indexes.vectorstore import (
    VectorstoreIndexCreator,
    VectorStoreIndexWrapper,
)
from langchain.llms.openai import OpenAI
from langchain.schema import Document


class AVectorStoreIndexWrapper(VectorStoreIndexWrapper):
    """Async wrapper around a vectorstore for easy access."""

    def __init__(
        self,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    async def aquery(
        self, question: str, llm: Optional[BaseLanguageModel] = None, **kwargs: Any
    ) -> str:
        """Query the vectorstore."""
        llm = llm or OpenAI(temperature=0)  # type: ignore[call-arg]
        chain = RetrievalQA.from_chain_type(
            llm, retriever=self.vectorstore.as_retriever(), **kwargs
        )
        return str(await chain.arun(question))


class AVectorstoreIndexCreator(VectorstoreIndexCreator):
    """Async logic for creating indexes."""

    def from_documents(self, documents: List[Document]) -> AVectorStoreIndexWrapper:
        """Create a vectorstore index from documents."""
        sub_docs = self.text_splitter.split_documents(documents)
        vectorstore = self.vectorstore_cls.from_documents(
            sub_docs, self.embedding, **self.vectorstore_kwargs
        )
        return AVectorStoreIndexWrapper(vectorstore=vectorstore)
