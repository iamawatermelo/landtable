from contextlib import contextmanager
import pytest_asyncio
from landtable.auth.abstract import Ruleset
from landtable.auth.reject import RejectAuthenticationContext
from landtable.core import Landtable
from landtable.core.models.config import ConfigurationModel
from landtable.identifiers import Identifier


TEST_WORKSPACE = Identifier.parse_from("lwk:eeaf52e770ed41f37e31a8ea738d46db")
TEST_NONEXISTENT_WORKSPACE = Identifier.parse_from("lwk:9f938f8ceebb9c0758e1e3e207bd2ffa")
TEST_WORKSPACE_ALIAS = "highseas"
TEST_NONEXISTENT_WORKSPACE_ALIAS = "nonexistent"


@pytest_asyncio.fixture(scope="session")
async def landtable():
    cfg = ConfigurationModel()
    lt = Landtable(cfg)
    async with lt.enter():
        yield lt


@pytest_asyncio.fixture(scope="session")
async def landtable_fi():
    """
    Test Landtable where it cannot connect to etcd
    """
    cfg = ConfigurationModel(
        etcd_hostname="127.0.0.2"
    )
    lt = Landtable(cfg)
    async with lt.enter():
        yield lt


@contextmanager
def auth(ruleset: list[Ruleset]):
    ctx = RejectAuthenticationContext()
    with ctx.extend(ruleset, False).enter():
        yield