"""
Landtable core.
"""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
from json import JSONDecodeError, loads, dumps
from time import monotonic
import aetcd
from typing import Callable

from pydantic import ValidationError
from landtable.auth.abstract import AccessType, AuthenticationContext, Resource, RuleType, Ruleset
from landtable.auth.abstract.resources import DatabaseConfigResource, TableRowsResource, WorkspaceAliasesResource, WorkspaceResource
from landtable.core.backends import DatabaseBackend, find_all_backends
from landtable.core.error_messages import DATABASE_NOT_FOUND, TABLE_NOT_FOUND, UNAVAILABLE_ETCD, WORKSPACE_NOT_FOUND, WRONG_NAMESPACE
from landtable.core.models.config import ConfigurationModel
from landtable.core.models.databases import DatabaseModel
from landtable.core.models.transactions import Transaction
from landtable.core.models.workspaces import WorkspaceModel
from landtable.exceptions import APIBadRequestException, APIForbidden, APINotFoundException, APIUnavailable
from landtable.identifiers import DatabaseIdentifier, Identifier, TableIdentifier, WorkspaceIdentifier

logger = logging.getLogger(__name__)

try:
    from orjson import JSONDecodeError, loads, dumps
except ImportError:
    logger.warn("orjson is not available (pip install landtable[speedups])")


class LandtableInternalException(Exception):
    pass


def require_no_auth_context[T: Callable](fn: T) -> T:
    """
    Decorator to ensure there is no current authentication context.
    """
    
    def _inner(*args, **kwargs):
        try:
            AuthenticationContext.from_context()
        except LookupError:
            pass
        else:
            raise LandtableInternalException(
                f"function {fn.__qualname__} prohibits using an authentication context"
            )
        
        return fn(*args, **kwargs)
    
    return _inner  # type: ignore


def require_auth_context[T: Callable](fn: T) -> T:
    """
    Decorator to ensure there is a current authentication context.
    """
    
    def _inner(*args, **kwargs):
        try:
            AuthenticationContext.from_context()
        except LookupError:
            pass
        else:
            return fn(*args, **kwargs)
        
        raise LandtableInternalException(
            f"function {fn.__qualname__} requires an authentication context"
        )
    
    return _inner  # type: ignore


@dataclass
class CacheEntry[T]:
    value: T
    issued_at: float


class Landtable():
    """
    The Landtable Core class.
    
    Note that most methods require authentication.
    """
    
    config: ConfigurationModel
    etcd: aetcd.Client
    
    workspaces: dict[WorkspaceIdentifier, CacheEntry[WorkspaceModel]]
    workspace_aliases: dict[str, CacheEntry[WorkspaceIdentifier]]
    
    database_plugins: dict[str, type[DatabaseBackend]]
    instantiated_database_plugins: dict[DatabaseIdentifier, DatabaseBackend]
    
    @require_no_auth_context
    def __init__(
        self,
        config: ConfigurationModel
    ):
        """
        Create a new instance of Landtable and validate its
        configuration.
        """
        
        self.config = config
        self.etcd = aetcd.Client(
            host=config.etcd_hostname,
            port=config.etcd_port,
            username=config.etcd_username,
            password=config.etcd_password
        )
        self.workspaces = dict()
        self.workspace_aliases = dict()
        self.database_plugins = find_all_backends()
    
    @require_no_auth_context
    async def _etcd_replicate_task(self):
        async for event in await self.etcd.watch(b"/landtable/"):
            event: aetcd.Event = event
            
            try:
                match event.kv.key.split(b"/")[2:]:
                    case [b"workspaces", workspace]:
                        try:
                            workspace_id = Identifier.parse_from_ns(
                                "lwk",
                                workspace.decode()
                            )
                        except (ValueError, UnicodeDecodeError):
                            logger.warning(f"Got invalid change event for {event.kv.key}: workspace ID {workspace} is invalid")
                            continue
                        
                        match event.kind:
                            case aetcd.EventKind.PUT:
                                try:
                                    workspace_json = loads(event.kv.value)
                                    parsed_workspace = WorkspaceModel(**workspace_json)
                                except (ValueError, TypeError, ValidationError) as e:
                                    logger.warning(f"Got invalid change event for {event.kv.key}: {e}")
                                    continue
                                
                                self.workspaces[workspace_id] = CacheEntry(
                                    value=parsed_workspace,
                                    issued_at=monotonic()
                                )
                            case aetcd.EventKind.DELETE:
                                del self.workspaces[workspace_id]
                            case _:
                                logger.warning(f"Unknown event kind {event.kind}")
                    case _:
                        logger.warning(f"Unknown change event {event.kv.key}")
            except Exception as e:
                logger.error(f"Uncaught exception while processing change event {event}: {e}")
    
    @require_no_auth_context
    @asynccontextmanager
    async def enter(self):
        """
        Asynchronous context manager to handle the Landtable instance
        lifecycle.
        """
        
        await self.etcd.connect()
        task = asyncio.create_task(self._etcd_replicate_task())
        
        yield
        
        task.cancel()
        await self.etcd.close()
    
    @require_auth_context
    async def resolve_workspace_alias(
        self,
        workspace: str
    ) -> WorkspaceIdentifier:
        """
        Given a workspace alias, find the workspace ID.
        
        SECURITY: Caller must have AccessType.READ permissions on
        WorkspaceAliasesResource().
                  
        The caller may be able to determine the existence of a workspace
        alias through looking at the execution time of this function.
        This is not a concern.
        """
        ctx = AuthenticationContext.from_context()
        
        with await ctx.evaluate({AccessType.READ}, WorkspaceAliasesResource()):
            if (entry := self.workspace_aliases.get(workspace)) is not None:
                if monotonic() - entry.issued_at < self.config.cache_expiry_time:
                    return entry.value
            
            try:
                alias = await self.etcd.get(f"/landtable/workspaceAliases/{workspace}".encode())
            except aetcd.exceptions.ClientError as e:
                logging.error(f"Could not connect to etcd: {e}")
                raise APIUnavailable(message=UNAVAILABLE_ETCD)
            
            if alias is None:
                raise APIForbidden(message=WORKSPACE_NOT_FOUND.format(
                    workspace=workspace
                ))
            
            try:
                alias_id = Identifier.parse_from_ns(
                    "lwk",
                    alias.value.decode()
                )
            except (ValueError, UnicodeDecodeError):
                raise LandtableInternalException(f"alias {workspace} -> {alias.value} is invalid")
            
        return alias_id
    
    @require_auth_context
    async def resolve_workspace(
        self,
        workspace: str | Identifier
    ):
        """
        Given an identifier, a string representing an identifier or a
        workspace alias, find a workspace identifier.
        
        SECURITY: If the workspace parameter is not an identifier,
        caller must have AccessType.READ permissions on
        WorkspaceAliasesResource().
        """
        ctx = AuthenticationContext.from_context()
        
        if isinstance(workspace, str):
            try:
                workspace = Identifier.parse_from(workspace)
            except ValueError:
                pass
        
        if isinstance(workspace, Identifier):
            if workspace.namespace != "lwk":
                raise APIBadRequestException(message=WRONG_NAMESPACE.format(
                    namespace="lwk",
                    identifier=workspace
                ))
        
        if isinstance(workspace, str):
            workspace = await self.resolve_workspace_alias(workspace)
        
        return workspace
    
    @require_auth_context
    async def fetch_workspace(
        self,
        workspace: str | Identifier
    ):
        """
        From a workspace identifier, string representing a workspace
        identifier or a workspace alias, fetch a workspace configuration.
        
        Note that workspaces that cannot be found will raise APIForbidden,
        not APINotFoundException.
        
        SECURITY: If the workspace parameter is not an identifier,
        caller must have AccessType.READ permissions on
        WorkspaceAliasesResource().
                  
        Caller must have AccessType.READ permissions on the
        WorkspaceResource for the retrieved workspace.
        """
        ctx = AuthenticationContext.from_context()
        workspace = await self.resolve_workspace(workspace)
        
        try:
            await ctx.evaluate(
                {AccessType.READ},
                WorkspaceResource(workspace=workspace)
            )
        except APIForbidden as e:
            e.message = WORKSPACE_NOT_FOUND.format(
                workspace=workspace
            )
            raise e
        
        try:
            workspace_bytes = await self.etcd.get(
                f"/landtable/workspaces/{workspace}/meta".encode(),
                serializable=True
            )
        except aetcd.exceptions.ClientError as e:
            logging.error(f"Could not connect to etcd: {e}")
            raise APIUnavailable(message=UNAVAILABLE_ETCD)
        
        if workspace_bytes is None:
            raise APIForbidden(message=WORKSPACE_NOT_FOUND.format(
                workspace=workspace
            ))
        
        try:
            workspace_model = WorkspaceModel(**loads(workspace_bytes.value))
        except (JSONDecodeError, UnicodeDecodeError):
            raise LandtableInternalException(f"workspace {workspace} contains malformed data")
        except ValidationError as e:
            raise LandtableInternalException(f"workspace {workspace} contains validation errors: {e}")
        
        return workspace_model
    
    
    @require_auth_context
    async def fetch_database(
        self,
        database: DatabaseIdentifier
    ):
        """
        Fetch a database by its identifier, resolving all secrets.
        Raises APINotFoundException if it does not exist.
        
        SECURITY: Caller must have AccessType.READ permissions on the
        DatabaseResource for this database.
        """
        
        ctx = AuthenticationContext.from_context()
        
        if database.namespace != "ldb":
            raise APIBadRequestException(message=WRONG_NAMESPACE.format(
                namespace="ldb",
                identifier=database
            ))
        
        try:
            await ctx.evaluate(
                {AccessType.READ},
                DatabaseConfigResource(database=database)
            )
        except APIForbidden as e:
            e.message = DATABASE_NOT_FOUND.format(
                database=database
            )
            raise e
        
        database_bytes = await self.etcd.get(
            f"/landtable/databases/{database}".encode(),
            serializable=True
        )
        
        if database_bytes is None:
            raise APIForbidden(message=DATABASE_NOT_FOUND.format(
                database=database
            ))
        
        try:
            database_model = DatabaseModel(**loads(database_bytes.value))
        except (JSONDecodeError, UnicodeDecodeError):
            raise LandtableInternalException(f"database {database} contains malformed data")
        except ValidationError as e:
            raise LandtableInternalException(f"database {database} contains validation errors: {e}")
        
        return database_model
    
    async def _resolve_database_plugin(
        self,
        database: DatabaseModel
    ):
        if (instantiated_plugin := self.instantiated_database_plugins.get(database.id)):
            return instantiated_plugin
        
        plugin = self.database_plugins.get(database.plugin)
        
        if plugin is None:
            return None
        
        instantiated_plugin = plugin(database.config)
        self.instantiated_database_plugins[database.id] = instantiated_plugin
        
        await instantiated_plugin.connect()
        
        return instantiated_plugin
    
    @require_auth_context
    async def execute_txn(
        self,
        workspace: WorkspaceModel,
        table: TableIdentifier,
        transaction: Transaction
    ):
        """
        Execute a transaction on a workspace table.
        
        SECURITY: Caller must have permission to access the table
        depending on what operations are called.
        
        Caller does not need permission to access the underlying 
        database.
        """
        
        ctx = AuthenticationContext.from_context()
        permissions = set()
        
        for op in transaction.ops:
            permissions |= type(op).access_types
        
        try:
            await ctx.evaluate(permissions, TableRowsResource(table=table))
        except APIForbidden as e:
            e.message = TABLE_NOT_FOUND.format(
                table=table
            )
            raise e
        
        table_model = workspace.tables.get(table)
        
        if table_model is None:
            raise APIForbidden(
                message=TABLE_NOT_FOUND.format(
                    table=table
                )
            )
        
        # SECURITY: Allow fetching the database configuration even if
        # the user can't access it. It is ensured that the database
        # configuration isn't passed back to the caller.
        with ctx.extend(
            rules=[
                Ruleset(
                    rule=RuleType.ALLOW,
                    actions={AccessType.READ},
                    test_resource=lambda x: x == DatabaseConfigResource(
                        database=workspace.primary
                    )
                )
            ],
            pass_through=False
        ).enter():
            primary_database_config = await self.fetch_database(workspace.primary)
        
        plugin = await self._resolve_database_plugin(primary_database_config)
        
        return await plugin.execute_txn(
            workspace=workspace,
            table=table,
            transaction=transaction
        )
