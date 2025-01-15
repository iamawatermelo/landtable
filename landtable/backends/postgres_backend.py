"""
A Landtable database backend that uses Postgres.

SECURITY:
    - All values passed in by an API call are properly escaped using query
      arguments. No data that has been taken from an API call will be
      concatenated into an SQL statement.

    - Column names taken from the replica's configuration aren't sanitized.
      It is expected that you have taken steps to secure your etcd cluster.
"""

# Copyright 2024 the Landtable authors
# https://github.com/iamawatermelo/landtable
# This file is part of Landtable and is shared under the Polyform Perimeter
# license version 1.0.1. See the LICENSE.md for more information.
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
from landtable.backends.abstract import TransactionOperation
from landtable.backends.abstract import Update
from landtable.backends.abstract import UpdateByFormula
from landtable.exceptions import APIBadRequestException
from landtable.formula.formula import Formula
from landtable.formula.parse import ASTConcreteType
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.sql import to_sql_expr
from landtable.formula.sql import to_sql_predicate
from landtable.formula.sql.functions import SQL_FUNCTIONS
from landtable.identifiers import DatabaseIdentifier
from landtable.identifiers import Identifier
from landtable.state.models import BaseLandtableDatabase
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtablePostgresV0Database
from landtable.state.models import LandtableTable
from landtable.tracing import Tracer
from landtable.tracing import wrap_trace

if TYPE_CHECKING:
    # from package asyncpg-stubs
    from asyncpg.transaction import _IsolationLevels
else:
    # to prevent the cast call from throwing unbound local error
    _IsolationLevels = None


logger = getLogger(__name__)


@wrap_trace("formula", "Create SQL predicate")
def parse_target(target: Target, environment: ASTTypeEnvironment, values: list[Any]):
    if type(target) is RowTarget:
        values.append(target.id.uuid)
        return f"{environment.id_field} = ${len(values)}"
    elif type(target) is FormulaTarget:
        return to_sql_predicate(target.formula, environment, values)
    else:
        raise NotImplementedError(f"got unexpected target {target}")


@wrap_trace("formula", "Create SELECT statement")
async def create_select_statement(
    target: Target,
    columns_to_select: set[str],
    replica: DatabaseIdentifier,
    table: LandtableTable,
    values: list,
    limit: int | None = None,
    sort: Formula | None = None,
):
    replica_config = table.fetch_replica_config(replica)
    assert replica_config.id_column is not None
    assert replica_config.created_at_column is not None

    env = ASTTypeEnvironment(
        variables={
            field.fetch_replica_config(replica).column_name: field.type_to_ast_type()
            for field in table.exposed_fields
        },
        functions=SQL_FUNCTIONS,
        id_field=replica_config.id_column,
        created_time_field=replica_config.created_at_column,
    )
    predicate = parse_target(target, env, values)
    # predicate is something like person = $1

    column_str = ",".join(columns_to_select)
    statement = (
        f"SELECT {column_str} FROM {replica_config.table_name}WHERE {predicate} "
    )

    if limit is not None:
        if limit <= 0:
            raise APIBadRequestException(
                message=f"limit must be above zero (got {limit})"
            )

        statement += f"LIMIT {limit}"

    if sort is not None:
        sort_predicate, type = to_sql_expr(sort, env, values)
        if type != ASTConcreteType.NUMBER:
            raise APIBadRequestException(
                message=f"sort formula expression should return a number, not {type}"
            )

        statement += f"ORDER BY {sort_predicate}"

    return statement


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

    async def _exec_rudop(
        self,
        op: Fetch | Update | UpdateByFormula | Delete,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: LandtableDatabase,
        connection: PoolConnectionProxy[Record],
    ):
        """
        Execute a fetch / update / delete operation.
        """

        replica_config = table.fetch_replica_config(table.id)
        assert replica_config.id_column is not None
        assert replica_config.created_at_column is not None

        columns = set()

        fields = table.resolve_fields(op.fields)

        for field in fields:
            columns.add(field.fetch_replica_config(table.id).column_name)

        columns.add(replica_config.id_column)
        columns.add(replica_config.created_at_column)

        values = list()
        query = await create_select_statement(
            op.target,
            # Add some more columns to join on if we're not just fetching
            columns if isinstance(op, Fetch) else columns | {"ctid", "tableoid"},
            table.id,
            table,
            values,
            op.limit,
            op.sort,
        )

        if isinstance(op, Update):
            if len(op.row) == 0:
                return RowResult(rows=[])

            field_map = table.create_field_map(op.row.keys())

            set_parts = list()

            for name, value in op.row.items():
                values.append(value)
                set_parts.append(f"{field_map[name]} = ${len(values)}")

            return_part = ",".join(
                f"old.{column} as {column}"
                for column in columns
                | {replica_config.id_column, replica_config.created_at_column}
            )

            query = (
                f"UPDATE {replica_config.table_name} AS new "
                f"SET {','.join(set_parts)} "
                f"FROM ({query}) AS old "
                "WHERE (new.ctid, new.tableoid) = (old.ctid, old.tableoid) "
                f"RETURNING {return_part}"
            )

        with Tracer.from_context().trace("db", "execute query", {"query": query}):
            result = await connection.fetch(query, *values, timeout=1)

        use_id = transaction.use_id

        return RowResult(
            rows=[
                Row(
                    id=Identifier("lrw", row.get(replica_config.id_column)),
                    created_at=row.get(replica_config.created_at_column),
                    contents={field.id if use_id else field.name for field in fields},
                )
                for row in result
            ]
        )

    async def _exec_op(
        self,
        op: BaseTransactionOperation,
        transaction: LandtableTransaction,
        table: LandtableTable,
        config: LandtableDatabase,
        connection: PoolConnectionProxy[Record],
    ):
        """
        Execute an operation.
        """
        op = cast(TransactionOperation, op)

        if type(op) in (Fetch, Update, Delete):
            return self._exec_rudop(op, transaction, table, config, connection)

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
                await self._exec_op(op, transaction, table, config, connection)
                for op in transaction.ops
            ]
