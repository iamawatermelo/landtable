from __future__ import annotations

import asyncio
from logging import getLogger
from typing import Any
from typing import cast
from typing import TYPE_CHECKING

import asyncpg
from asyncpg import Pool
from asyncpg import Record
from asyncpg.pool import PoolConnectionProxy

from landtable.backends.abstract import BackendInformation
from landtable.backends.abstract import BaseTransactionOperation
from landtable.backends.abstract import DatabaseBackend
from landtable.backends.abstract import Delete
from landtable.backends.abstract import Fetch
from landtable.backends.abstract import FormulaTarget
from landtable.backends.abstract import LandtableTransaction
from landtable.backends.abstract import Row
from landtable.backends.abstract import RowResult
from landtable.backends.abstract import RowTarget
from landtable.backends.abstract import Target
from landtable.backends.abstract import TransactionConsistency
from landtable.backends.abstract import TransactionConsistencyEmulation
from landtable.exceptions import APIBadRequestException
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.sql import to_sql
from landtable.formula.sql.functions import SQL_FUNCTIONS
from landtable.identifiers import Identifier
from landtable.state.models import BaseLandtableDatabase
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtableField
from landtable.state.models import LandtablePostgresV0Database
from landtable.state.models import LandtableTable
from landtable.tracing import Tracer

if TYPE_CHECKING:
    # from package asyncpg-stubs
    from asyncpg.transaction import _IsolationLevels
else:
    # to prevent the cast call from throwing unbound local error
    _IsolationLevels = None


logger = getLogger(__name__)


def parse_target(target: Target, environment: ASTTypeEnvironment, values: list[Any]):
    if type(target) is RowTarget:
        values.append(target.id.uuid)
        return f"{environment.id_field} = ${len(values)}"
    elif type(target) is FormulaTarget:
        return to_sql(target.formula, environment, values)
    else:
        raise NotImplementedError(f"got unexpected target {target}")


class PostgresBackend(DatabaseBackend):
    BACKEND_INFORMATION = BackendInformation(
        transaction_type=TransactionConsistencyEmulation.STRONG,
        config_types={"postgres_v0"},
    )

    pools: dict[str, Pool[Record]]

    def __init__(self):
        self.pools = dict()

    async def fetch_connection_pool(self, url: str):
        if (pool := self.pools.get(url)) is not None:
            return pool

        new_pool = await asyncpg.create_pool(url)
        assert new_pool  # why is the inferred type Pool | None ??
        self.pools[url] = new_pool

        return new_pool

    async def _exec_op(
        self,
        op: BaseTransactionOperation,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: LandtableDatabase,
        consistency: TransactionConsistency,
        connection: PoolConnectionProxy[Record],
    ):
        logger.debug(f"exec op {op}")

        if type(op) is Fetch or type(op) is Delete:
            if op.execTarget is not None and op.failType is None:
                raise APIBadRequestException(
                    message="execTarget specified but no operator to compare with"
                )

            with Tracer.from_context().trace("parse", f"parse {type(op).__qualname__}"):
                replica_config = table.fetch_replica_config(config.id)

                if (
                    replica_config.id_column is None
                    or replica_config.created_at_column is None
                ):
                    raise Exception(
                        f"Invalid backend configuration for table {config.id}"
                    )

                values = list()
                predicate = parse_target(
                    op.target,
                    ASTTypeEnvironment(
                        variables={
                            field.fetch_replica_config(
                                config.id
                            ).column_name: field.type_to_ast_type()
                            for field in table.exposed_fields
                        },
                        functions=SQL_FUNCTIONS,
                        id_field=replica_config.id_column,
                        created_time_field=replica_config.created_at_column,
                    ),
                    values,
                )
                db_table = replica_config.table_name
                columns = table.resolve_columns(op.fields)
                db_columns = set(
                    x.fetch_replica_config(config.id).column_name for x in columns
                )
                db_columns.add(replica_config.id_column)
                db_columns.add(replica_config.created_at_column)
                db_column_str = ",".join(db_columns)

                if type(op) is Delete:
                    db_column_str = "ctid"

                query = f"SELECT {db_column_str} FROM {db_table} WHERE {predicate} LIMIT {op.limit}"

                if type(op) is Delete:
                    db_column_str = ",".join(db_columns)
                    query = f"DELETE FROM {db_table} WHERE ctid = ANY(ARRAY({query})) RETURNING {db_column_str}"

            with Tracer.from_context().trace(
                "db", f"execute {query}", {"values": repr(values)}
            ):
                result = await connection.fetch(query, *values)

                logger.debug(f"result: {result}")

                return RowResult(
                    rows=[
                        Row(
                            id=Identifier("lrw", record[replica_config.id_column]),
                            created_at=record[replica_config.created_at_column],
                            contents={
                                (
                                    str(column.id)
                                    if transaction.use_id
                                    else column.name
                                ): record[
                                    column.fetch_replica_config(config.id).column_name
                                ]
                                for column in columns
                            },
                        )
                        for record in result
                    ]
                )
        else:
            raise NotImplementedError

    async def exec_transaction(
        self,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: BaseLandtableDatabase,
        *,
        consistency: TransactionConsistency = TransactionConsistency.STRICT,
    ):
        assert config.type == "postgres_v0"
        config = cast(LandtablePostgresV0Database, config)

        with Tracer.from_context().trace("db", f"connect to {config.connection_url}"):
            pool = await self.fetch_connection_pool(config.connection_url)
            connection = await pool.acquire()

        async with connection.transaction(
            isolation=cast(
                _IsolationLevels,
                {
                    TransactionConsistency.STRICT: "serializable",
                    TransactionConsistency.RELAXED: "repeatable_read",
                    TransactionConsistency.NONE: "read_committed",
                }[consistency],
            ),
            readonly=transaction.read_only,
            deferrable=True,
        ):
            # TODO: Run multiple in parallel (if that's even possible)
            return [
                await self._exec_op(
                    op, transaction, table, config, consistency, connection
                )
                for op in transaction.ops
            ]
