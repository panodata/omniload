import unittest

import dlt
from sqlalchemy.util import classproperty

from omniload.target.cratedb import CrateDBDestination
from tests.main.test_targets import GenericSqlDestinationFixture


class CrateDBDestinationTest(unittest.TestCase, GenericSqlDestinationFixture):
    destination = CrateDBDestination()

    @classproperty
    def expected_class(cls):
        return dlt.destinations.cratedb  # ty: ignore[unresolved-attribute]
