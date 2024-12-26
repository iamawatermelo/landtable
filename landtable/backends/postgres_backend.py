import asyncio
from dataclasses import dataclass
from typing import Any
from typing import cast

import asyncpg
from asyncpg import Connection
from asyncpg import Pool
from asyncpg import Record
from asyncpg.pool import PoolConnectionProxy
from asyncpg.transaction import ISOLATION_LEVELS
from asyncpg.transaction import ISOLATION_LEVELS_BY_VALUE

from landtable.backends.abstract import BaseTransactionOperation
from landtable.backends.abstract import DatabaseBackend
from landtable.backends.abstract import EmulatedTransactionType
from landtable.backends.abstract import Fetch
from landtable.backends.abstract import FormulaTarget
from landtable.backends.abstract import LandtableTransaction
from landtable.backends.abstract import Row
from landtable.backends.abstract import RowResult
from landtable.backends.abstract import RowTarget
from landtable.backends.abstract import Target
from landtable.backends.abstract import TransactionOperation
from landtable.formula.formula import Formula
from landtable.formula.parse import ASTTypeEnvironment
from landtable.formula.sql import to_sql
from landtable.formula.sql.functions import SQL_FUNCTIONS
from landtable.identifiers import Identifier
from landtable.state.models import LandtableDatabase
from landtable.state.models import LandtablePostgresV0Database
from landtable.state.models import LandtableTable
from landtable.state.models import LandtableWorkspace
from landtable.tracing import Tracer


def parse_target(target: Target, environment: ASTTypeEnvironment, values: list[Any]):
    if type(target) is RowTarget:
        values.append(target.id.uuid)
        return f"{environment.id_field} = ${len(values) + 1}"
    elif type(target) is FormulaTarget:
        return to_sql(target.formula, environment, values)
    else:
        raise NotImplementedError(f"got unexpected target {target}")


class PostgresBackend(DatabaseBackend):
    pools: dict[str, Pool[Record]]

    async def setup(self):
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
        consistency: EmulatedTransactionType | None,
        connection: PoolConnectionProxy[Record],
    ):
        if type(op) is Fetch:
            with Tracer.from_context().trace("parse", f"parse {op.__name__}"):
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
                        id_field=table.id_field,
                        created_time_field=table.created_time_field,
                    ),
                    values,
                )
                columns = op.resolve_columns(table)
                db_columns = ",".join(
                    x.fetch_replica_config(config.id).column_name for x in columns
                )
                replica_config = table.fetch_replica_config(config.id)

                assert replica_config.id_column
                assert replica_config.created_at_column

                db_table = replica_config.table_name
                query = f"SELECT {db_columns} FROM {db_table} WHERE {predicate} LIMIT {op.limit}"

            with Tracer.from_context().trace("db", f"execute {query}"):
                result = await connection.fetch(query, values)

                return RowResult(
                    rows=[
                        Row(
                            id=Identifier("lrw", record[replica_config.id_column]),
                            created_at=record[replica_config.created_at_column],
                            contents={
                                (
                                    column.lt_id
                                    if transaction.use_id
                                    else column.lt_name
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
        config: LandtableDatabase,
        *,
        consistency: EmulatedTransactionType | None,
    ):
        assert config.type == "postgres_v0"
        config = cast(LandtablePostgresV0Database, config)

        with Tracer.from_context().trace("db", f"connect to {config.connection_url}"):
            pool = await self.fetch_connection_pool(config.connection_url)
            connection = await pool.acquire()

        async with connection.transaction(
            isolation="serializable", readonly=transaction.read_only, deferrable=True
        ):
            return await asyncio.gather(
                *(
                    self._exec_op(
                        op, transaction, table, config, consistency, connection
                    )
                    for op in transaction.ops
                )
            )
