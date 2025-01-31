import pytest
from tests import TEST_WORKSPACE, TEST_WORKSPACE_ALIAS, TEST_NONEXISTENT_WORKSPACE_ALIAS, TEST_NONEXISTENT_WORKSPACE, auth
from landtable.auth.abstract import AccessType, RuleType, Ruleset
from landtable.auth.abstract.resources import WorkspaceAliasesResource, WorkspaceResource
from landtable.core import Landtable, LandtableInternalException
from landtable.exceptions import APIForbidden, APIUnavailable

pytestmark = pytest.mark.asyncio(loop_scope="session")


async def test_fetch_workspace_requires_ctx(landtable: Landtable):
    with pytest.raises(LandtableInternalException):
        await landtable.fetch_workspace(TEST_WORKSPACE)


async def test_fetch_workspace_requires_access(landtable: Landtable):
    with (
        pytest.raises(APIForbidden),
        auth([])
    ):
        await landtable.fetch_workspace(TEST_WORKSPACE)
    
    with auth([
        Ruleset(
            rule=RuleType.ALLOW,
            actions={AccessType.READ},
            test_resource=lambda x: x == WorkspaceResource(
                workspace=TEST_NONEXISTENT_WORKSPACE
            )
        )
    ]):
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


async def test_fetch_workspace_data_leakage(landtable: Landtable):
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