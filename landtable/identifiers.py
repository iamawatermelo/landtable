"""
Utilities for handling Landtable identifiers.
"""

from typing import Annotated
from typing import Any
from typing import cast
from typing import Literal
from typing import TypeAlias
from typing import Union
from uuid import UUID

from pydantic import AfterValidator
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

        if to_parse[4] != ":":
            raise ValueError("identifier should be delimited with :")

        if len(to_parse) != 20:
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
        return f"{self.namespace}:{self.uuid}"

    def __hash__(self) -> int:
        return hash(self.uuid)

    @classmethod
    def validate(cls, value: str, info: ValidationInfo):
        return cls.parse_from(value)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_after_validator_function(
            cls.validate, handler(str)
        )


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
