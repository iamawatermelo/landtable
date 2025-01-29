import asyncio
from contextlib import asynccontextmanager, contextmanager
import pytest
import pytest_asyncio

from landtable.auth.abstract import AccessType, RuleType, Ruleset
from landtable.auth.abstract.resources import WorkspaceAliasesResource, WorkspaceResource
from landtable.auth.reject import RejectAuthenticationContext
from landtable.core import Landtable, LandtableInternalException
from landtable.core.models.config import ConfigurationModel
from landtable.exceptions import APIForbidden, APIUnavailable
from landtable.identifiers import Identifier

pytestmark = pytest.mark.asyncio(loop_scope="session")

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


async def test_fetch_workspace_requires_ctx(landtable: Landtable):
    with pytest.raises(LandtableInternalException):
        await landtable.fetch_workspace(TEST_WORKSPACE)


async def test_fetch_workspace_requires_access(landtable: Landtable):
    with (
        pytest.raises(APIForbidden),
        auth([])
    ):
        await landtable.fetch_workspace(TEST_WORKSPACE)


async def test_fetch_workspace(landtable: Landtable):
    with auth([
        Ruleset(
            rule=RuleType.ALLOW,
            actions={AccessType.READ},
            test_resource=lambda x: x == WorkspaceResource(
                workspace=TEST_WORKSPACE
            )
        )
    ]):
        await landtable.fetch_workspace(TEST_WORKSPACE)


async def test_fi_fetch_workspace(landtable_fi: Landtable):
    with (
        pytest.raises(APIUnavailable),
        auth([
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceResource(
                    workspace=TEST_WORKSPACE
                )
            )
        ])
    ):
        await landtable_fi.fetch_workspace(TEST_WORKSPACE)


async def test_fetch_workspace_alias_requires_access(landtable: Landtable):
    with (
        pytest.raises(APIForbidden),
        auth([
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceResource(
                    workspace=TEST_WORKSPACE
                )
            )
        ])
    ):
        await landtable.fetch_workspace(TEST_WORKSPACE_ALIAS)


async def test_fetch_workspace_alias(landtable: Landtable):
    with (
        auth([
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceResource(
                    workspace=TEST_WORKSPACE
                )
            ),
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceAliasesResource()
            )
        ])
    ):
        await landtable.fetch_workspace(TEST_WORKSPACE_ALIAS)


async def test_fi_fetch_workspace_alias(landtable_fi: Landtable):
    with (
        pytest.raises(APIUnavailable),
        auth([
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceResource(
                    workspace=TEST_WORKSPACE
                )
            ),
            Ruleset(
                rule=RuleType.ALLOW,
                actions={AccessType.READ},
                test_resource=lambda x: x == WorkspaceAliasesResource()
            )
        ])
    ):
        await landtable_fi.fetch_workspace(TEST_WORKSPACE_ALIAS)