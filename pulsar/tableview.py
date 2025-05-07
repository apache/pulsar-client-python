#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
The TableView implementation.
"""

from typing import Any, Callable, Optional
from pulsar.schema.schema import Schema
import _pulsar

class TableView():

    def __init__(self, table_view: _pulsar.TableView, topic: str,
                 subscription: Optional[str], schema: Schema) -> None:
        self._table_view = table_view
        self._topic = topic
        self._subscription = subscription
        self._schema = schema

    def get(self, key: str) -> Optional[Any]:
        """
        Return the value associated with the given key in the table view.

        Parameters
        ----------
        key: str
            The message key

        Returns
        -------
        Optional[Any]
            The value associated with the key, or None if the key does not exist.
        """
        pair = self._table_view.get(key)
        if pair[0]:
            return self._schema.decode(pair[1])
        else:
            return None

    def for_each(self, callback: Callable[[str, Any], None]) -> None:
        """
        Iterate over all entries in the table view and call the callback function
        with the key and value for each entry.

        Parameters
        ----------
        callback: Callable[[str, Any], None]
            The callback function to call for each entry.
        """
        self._table_view.for_each(lambda k, v: callback(k, self._schema.decode(v)))

    def for_each_and_listen(self, callback: Callable[[str, Any], None]) -> None:
        """
        Iterate over all entries in the table view and call the callback function
        with the key and value for each entry, then listen for changes. The callback
        will be called when a new entry is added or an existing entry is updated.

        Parameters
        ----------
        callback: Callable[[str, Any], None]
            The callback function to call for each entry.
        """
        self._table_view.for_each_and_listen(lambda k, v: callback(k, self._schema.decode(v)))

    def close(self) -> None:
        """
        Close the table view.
        """
        self._table_view.close()

    def __len__(self) -> int:
        """
        Return the number of entries in the table view.
        """
        return self._table_view.size()

    def __str__(self) -> str:
        if self._subscription is None:
            return f"TableView(topic={self._topic})"
        else:
            return f"TableView(topic={self._topic}, subscription={self._subscription})"

    def __repr__(self) -> str:
        return self.__str__()
