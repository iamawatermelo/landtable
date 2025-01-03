"""
Utilities for handling Landtable identifiers.
"""

from __future__ import annotations

from typing import Annotated, Self
from typing import Any
from typing import cast
from typing import Literal
from typing import TypeAlias
from typing import Union
from uuid import UUID

from pydantic import AfterValidator, GetJsonSchemaHandler
from pydantic import GetCoreSchemaHandler
from pydantic import ValidationInfo
from pydantic_core import core_schema
from pydantic_core import CoreSchema


IdentifierNamespace: TypeAlias = Union[
    Literal["ltb"], Literal["lfd"], Literal["lwk"], Literal["lrw"], Literal["ldb"]
]


class Identifier:
    namespace: IdentifierNamespace
    uuid: UUID

    def __init__(self, namespace: IdentifierNamespace, uuid: UUID):
        self.namespace = namespace
        self.uuid = uuid

    @classmethod
    def parse_from(cls, to_parse: str):
        """
        Parse an identifier from a string, like
        """

        if to_parse[3] != ":":
            raise ValueError("identifier should be delimited with :")

        if len(to_parse) != 36:
            raise ValueError("identifier has invalid length")

        return cls(cast(IdentifierNamespace, to_parse[:3]), UUID(hex=to_parse[4:]))

    @classmethod
    def parse_from_ns(cls, namespace: IdentifierNamespace, to_parse: str):
        identifier = cls.parse_from(to_parse)
        if identifier.namespace != namespace:
            raise ValueError(
                f"expected identifier with namespace {namespace} (got {identifier.namespace})"
            )

        return identifier

    def __repr__(self):
        return f"{self.namespace}:{self.uuid.hex}"

    def __hash__(self) -> int:
        return hash(self.uuid.bytes)

    def __eq__(self, value: object, /) -> bool:
        return (
            isinstance(value, Identifier)
            and self.uuid == value.uuid
            and self.namespace == value.namespace
        )

    @classmethod
    def validate(cls, value: str | Self, info: ValidationInfo):
        if isinstance(value, Identifier):
            return value

        return cls.parse_from(value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        def serialize(instance: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                return repr(instance)

            return instance

        return core_schema.with_info_after_validator_function(
            cls.validate,
            core_schema.union_schema(
                [handler(str), core_schema.is_instance_schema(cls)]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                serialize, info_arg=True
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: CoreSchema, handler: GetJsonSchemaHandler
    ):
        return handler(core_schema.str_schema())


def identifier_validator_factory(namespace: IdentifierNamespace):
    def validator(identifier: Identifier) -> Identifier:
        if identifier.namespace != namespace:
            raise ValueError(
                f"expected identifier with namespace {namespace} (got {identifier.namespace})"
            )

        return identifier

    return validator


TableIdentifier: TypeAlias = Annotated[
    Identifier, AfterValidator(identifier_validator_factory("ltb"))
]

WorkspaceIdentifier: TypeAlias = Annotated[
    Identifier, AfterValidator(identifier_validator_factory("lwk"))
]

FieldIdentifier: TypeAlias = Annotated[
    Identifier, AfterValidator(identifier_validator_factory("lfd"))
]

RowIdentifier: TypeAlias = Annotated[
    Identifier, AfterValidator(identifier_validator_factory("lrw"))
]

DatabaseIdentifier: TypeAlias = Annotated[
    Identifier, AfterValidator(identifier_validator_factory("ldb"))
]
