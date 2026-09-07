# Copyright 2025-2026 Pricefx
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Pricefx query-related domain objects validators."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from enum import StrEnum, unique
from typing import Any, Literal, Optional

from pydantic import alias_generators, BaseModel, ConfigDict, Field


@unique
class Operator(StrEnum):
    """Logical operators for combining filter criteria."""

    AND = "and"
    OR = "or"
    NOT = "not"

class OperationAgg(StrEnum):
    """Operation aggregate class."""

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_ALL = "countAll"
    COUNT_NON_NULL = "countNonNull"
    COUNT_DISTINCT_NON_NULL = "countDistinctNonNull"

class Operation(StrEnum):
    """Operation class."""

    AND = "and"
    OR = "or"
    NOT = "not"
    ABS = "abs"
    EXP = "exp"
    CASE_WHEN = "caseWhen"
    COALESCE = "coalesce"
    NULL_IF = "nullIf"
    LESS_THAN = "lessThan"
    LESS_OR_EQUAL = "lessOrEqual"
    GREATER_THAN = "greaterThan"
    GREATER_OR_EQUAL = "greaterOrEqual"
    EQUAL = "equal"
    EQUAL_NULL_AWARE = "equalNullAware"
    NOT_EQUAL = "notEqual"
    NOT_EQUAL_NULL_AWARE = "notEqualNullAware"
    LIKE = "like"
    ILIKE = "ilike"
    NOT_LIKE = "notLike"
    NOT_ILIKE = "notIlike"
    IN = "in"
    NOT_IN = "notIn"
    IS_NULL = "isNull"
    IS_NOT_NULL = "isNotNull"
    IS_TRUE = "isTrue"
    IS_FALSE = "isFalse"
    CONCAT = "concat"
    SUBSTRING = "substring"
    UPPER = "upper"
    LOWER = "lower"
    TRIM = "trim"
    LENGTH = "length"
    LOCATE = "locate"
    PLUS = "plus"
    MINUS = "minus"
    MULTIPLY = "multiply"
    DIV = "div"
    MOD = "mod"
    CAST_AS_INTEGER = "castAsInteger"
    CAST_AS_BOOLEAN = "castAsBoolean"
    CAST_AS_DATE_ONLY = "castAsDateOnly"
    CAST_AS_DATE_TIME = "castAsDateTime"
    CAST_AS_REAL = "castAsReal"
    CAST_AS_STRING = "castAsString"

class FilterOperator(StrEnum):
    """Filter operators for search criteria."""

    EQUALS = "equals"
    IEQUALS = "iEquals"
    NOTEQUAL = "notEqual"
    INOTEQUAL = "iNotEqual"
    GREATERTHAN = "greaterThan"
    GREATEROREQUAL = "greaterOrEqual"
    LESSOREQUAL = "lessOrEqual"
    LESSTHAN = "lessThan"
    ISNULL = "isNull"
    NOTNULL = "notNull"
    CONTAINS = "contains"
    ICONTAINS = "iContains"
    CONTAINSPATTERN = "containsPattern"
    ICONTAINSPATTERN = "iContainsPattern"
    NOTCONTAINS = "notContains"
    INOTCONTAINS = "iNotContains"
    STARTSWITH = "startsWith"
    ISTARTSWITH = "iStartsWith"
    NOTSTARTSWITH = "notStartsWith"
    INOTSTARTSWITH = "iNotStartsWith"
    ENDSWITH = "endsWith"
    IENDSWITH = "iEndsWith"
    NOTENDSWITH = "notEndsWith"
    INOTENDSWITH = "iNotEndsWith"
    BETWEEN = "between"
    BETWEENINCLUSIVE = "betweenInclusive"
    IBETWEEN = "iBetween"
    IBETWEENINCLUSIVE = "iBetweenInclusive"
    INSET = "inSet"
    NOTINSET = "notInSet"


NUMERIC_FILTERS = [
    FilterOperator.EQUALS,
    FilterOperator.NOTEQUAL,
    FilterOperator.GREATERTHAN,
    FilterOperator.LESSTHAN,
    FilterOperator.GREATEROREQUAL,
    FilterOperator.LESSOREQUAL,
    FilterOperator.ISNULL,
    FilterOperator.NOTNULL,
    FilterOperator.BETWEEN,
    FilterOperator.BETWEENINCLUSIVE,
    FilterOperator.INSET,
    FilterOperator.NOTINSET,
]
DATE_FILTERS = [
    FilterOperator.EQUALS,
    FilterOperator.NOTEQUAL,
    FilterOperator.GREATERTHAN,
    FilterOperator.LESSTHAN,
    FilterOperator.GREATEROREQUAL,
    FilterOperator.LESSOREQUAL,
    FilterOperator.ISNULL,
    FilterOperator.NOTNULL,
    FilterOperator.BETWEEN,
    FilterOperator.BETWEENINCLUSIVE,
]
STRING_FILTERS = [
    FilterOperator.EQUALS,
    FilterOperator.IEQUALS,
    FilterOperator.NOTEQUAL,
    FilterOperator.INOTEQUAL,
    FilterOperator.ISNULL,
    FilterOperator.NOTNULL,
    FilterOperator.CONTAINS,
    FilterOperator.ICONTAINS,
    FilterOperator.CONTAINSPATTERN,
    FilterOperator.ICONTAINSPATTERN,
    FilterOperator.NOTCONTAINS,
    FilterOperator.INOTCONTAINS,
    FilterOperator.STARTSWITH,
    FilterOperator.ISTARTSWITH,
    FilterOperator.NOTSTARTSWITH,
    FilterOperator.INOTSTARTSWITH,
    FilterOperator.ENDSWITH,
    FilterOperator.IENDSWITH,
    FilterOperator.NOTENDSWITH,
    FilterOperator.INOTENDSWITH,
    FilterOperator.IBETWEEN,
    FilterOperator.IBETWEENINCLUSIVE,
    FilterOperator.INSET,
    FilterOperator.NOTINSET,
]


class FieldRule(BaseModel):
    """FieldRule class for query criteria on a given field."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )

    field_name: str
    operator: FilterOperator
    value: Optional[Any] = None
    start: Optional[Any] = None
    end: Optional[Any] = None


class AdvancedCriteria(BaseModel):
    """AdvancedCriteria class for combining multiple FieldRules with logical operators."""

    model_config = ConfigDict(serialize_by_alias=True, validate_by_name=True)

    operator: Operator
    criteria: Sequence[FieldRule | AdvancedCriteria]
    constructor: Literal["AdvancedCriteria"] = Field(
        default="AdvancedCriteria", alias="_constructor"
    )

    @classmethod
    def from_dict(
        cls, filters: dict[str, Any], filter_aggregator: Operator = Operator.AND
    ) -> AdvancedCriteria:
        """Build an equality criteria from a simple `{field: value}` dict.

        Args:
            filters: mapping of field name to expected value (equality match)
            filter_aggregator: how the per-field equality checks are combined (default: `and`)
        """
        return cls(
            operator=filter_aggregator,
            criteria=[
                FieldRule(field_name=field_name, operator=FilterOperator.EQUALS, value=value)
                for field_name, value in filters.items()
            ],
        )

    @classmethod
    def from_filters(
        cls,
        filters: dict[str, Any] | Sequence[FieldRule] | AdvancedCriteria,
        filter_aggregator: Operator = Operator.AND,
    ) -> AdvancedCriteria:
        """Normalize a `dict`, a sequence of `FieldRule` or an `AdvancedCriteria`.

        Args:
            filters: a `{field: value}` dict (equality match per key), a sequence of
                `FieldRule`, or an already-built `AdvancedCriteria` (returned as-is).
            filter_aggregator: how multiple conditions are combined, for the
                `dict`/sequence forms (default: `and`). Ignored for `AdvancedCriteria`.
        """
        if isinstance(filters, AdvancedCriteria):
            return filters
        if isinstance(filters, dict):
            return cls.from_dict(filters, filter_aggregator)
        return cls(operator=filter_aggregator, criteria=list(filters))


AdvancedCriteria.model_rebuild()


class LiteralType(StrEnum):
    """Type of literal value in a query result column."""

    INTEGER = "INTEGER"
    REAL = "REAL"
    DATE_ONLY = "DATE_ONLY"
    DATE_TIME = "DATE_TIME"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    OTHER = "OTHER"

    def to_py_type(self) -> Any:
        """Convert the LiteralType to a Python type."""
        if self == LiteralType.DATE_ONLY:
            return date
        if self == LiteralType.DATE_TIME:
            return datetime
        if self == LiteralType.INTEGER:
            return int
        if self == LiteralType.REAL:
            return float
        if self == LiteralType.BOOLEAN:
            return bool
        # Fall back to str parsing by default
        return str

    def to_pandas_type(self) -> Any:
        """Convert the LiteralType to a Pandas type.

        Same as to_py_type, except that date and datetime types are converted to datetime64[ns].
        """
        if self in [LiteralType.DATE_ONLY, LiteralType.DATE_TIME]:
            return "datetime64[ns]"
        if self == LiteralType.INTEGER:
            return int
        if self == LiteralType.REAL:
            return float
        if self == LiteralType.BOOLEAN:
            return bool
        # Fall back to str parsing by default
        return str


class Products(BaseModel):
    """Products table."""

    kind: Literal["products"] = "products"


class ProductExtensionRows(BaseModel):
    """Product extension rows table."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )

    kind: Literal["productExtensionRows"] = "productExtensionRows"
    product_extension_name: str


class CompanyParametersRows(BaseModel):
    """Company parameters rows table."""

    kind: Literal["companyParameterRow"] = "companyParameterRow"
    name: str


class PADataSource(BaseModel):
    """Data source table."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )

    kind: Literal["datasource"] = "datasource"
    data_source_unique_name: str


class PADatamart(BaseModel):
    """Datamart table."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )

    kind: Literal["datamart"] = "datamart"
    datamart_unique_name: str
    currency: Optional[str] = None
    uom: Optional[str] = None


class PADataFeed(BaseModel):
    """Data feed table."""

    model_config = ConfigDict(
        serialize_by_alias=True, validate_by_name=True, alias_generator=alias_generators.to_camel
    )

    kind: Literal["datafeed"] = "datafeed"
    data_feed_unique_name: str


Table = Products | ProductExtensionRows | PADataSource | PADatamart | PADataFeed


# All Selectables


class SourceColumnReference(BaseModel):
    """Source column class."""

    kind: Literal["columnReference"] = "columnReference"
    column: str
    source: Literal["table"] = "table"


class PreviousStageColumnReference(BaseModel):
    """Previous stage column class."""

    kind: Literal["columnReference"] = "columnReference"
    column: str
    source: Literal["previousStage"] = "previousStage"


class LiteralValue(BaseModel):
    """Literal class."""

    kind: Literal["literal"] = "literal"
    type: LiteralType
    value: Any


class PreviousStageFunction(BaseModel):
    """Function class."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["PreviousStageExpression"]


PreviousStageExpression: TypeAlias = (
        LiteralValue | PreviousStageColumnReference | PreviousStageFunction
)


class SourceFunction(BaseModel):
    """Source Function class."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["SourceExpression"]


SourceExpression: TypeAlias = LiteralValue | SourceColumnReference | SourceFunction


class SourceSelectable(BaseModel):
    """Source Selectable class."""

    kind: Literal["selectable"] = "selectable"
    expression: SourceExpression
    alias: str


class PreviousStageSelectable(BaseModel):
    """Selectable class."""

    kind: Literal["selectable"] = "selectable"
    expression: PreviousStageExpression
    alias: str


# Special case for aggregable expressions


class PreviousStageFunctionAgg(BaseModel):
    """Function agg class."""

    kind: Literal["function"] = "function"
    name: OperationAgg
    arguments: list[PreviousStageExpression]


ExpressionAgg: TypeAlias = (
        LiteralValue
        | PreviousStageColumnReference
        | PreviousStageFunctionAgg
        | PreviousStageFunction
)


class PreviousStageSelectableAgg(BaseModel):
    """Selectable agg class."""

    kind: Literal["selectable"] = "selectable"
    expression: ExpressionAgg
    alias: str


# All the stage types


class Source(BaseModel):
    """Source class."""

    kind: Literal["source"] = "source"
    table: Table
    columns: Optional[list[SourceSelectable]] = None
    # Note: We disable this field because the LLM has trouble generating proper criteria
    # instead we rely on explicity filter stage for now.
    # criteria: Optional[SourceExpression] = None


class JoinType(StrEnum):
    """Join type class."""

    INNER = "INNER"
    LEFT_OUTER = "LEFT_OUTER"


class JoinFunction(BaseModel):
    """Function class for joins."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["JoinExpression"]


JoinExpression: TypeAlias = (
        LiteralValue | PreviousStageColumnReference | SourceColumnReference | JoinFunction
)


class Join(BaseModel):
    """Inner join class."""

    kind: Literal["join"] = "join"
    table: Table
    type: JoinType
    columns: list[SourceSelectable]
    criteria: JoinExpression


class AddColumns(BaseModel):
    """Add columns class."""

    kind: Literal["addColumns"] = "addColumns"
    columns: list[PreviousStageSelectable]


class RemoveColumns(BaseModel):
    """Remove columns class."""

    kind: Literal["removeColumns"] = "removeColumns"
    columns: list[str]


class RetainColumns(BaseModel):
    """Retain columns class."""

    kind: Literal["retainColumns"] = "retainColumns"
    columns: list[str]


class Filter(BaseModel):
    """Filter class."""

    kind: Literal["filter"] = "filter"
    criteria: PreviousStageExpression


class Aggregate(BaseModel):
    """Aggregate class."""

    kind: Literal["aggregate"] = "aggregate"
    columns: list[PreviousStageSelectableAgg]
    dimensions: list[PreviousStageExpression]


class Distinct(BaseModel):
    """Distinct class."""

    kind: Literal["distinct"] = "distinct"


class Take(BaseModel):
    """Take class."""

    kind: Literal["take"] = "take"
    count: int


class OrderDirection(StrEnum):
    """Order direction class."""

    ASC_NULLS_FIRST = "ASC_NULLS_FIRST"
    ASC_NULLS_LAST = "ASC_NULLS_LAST"
    DESC_NULLS_FIRST = "DESC_NULLS_FIRST"
    DESC_NULLS_LAST = "DESC_NULLS_LAST"


class Order(BaseModel):
    """Order class."""

    kind: Literal["order"] = "order"
    expression: PreviousStageExpression
    direction: OrderDirection


class Sort(BaseModel):
    """Sort class."""

    kind: Literal["sort"] = "sort"
    orders: list[Order]


Stage: TypeAlias = (
        Source
        | Join
        | AddColumns
        | RemoveColumns
        | RetainColumns
        | Filter
        | Aggregate
        | Distinct
        | Sort
        | Take
)

###


class Pipeline(BaseModel):
    """Pipeline class."""

    kind: Literal["pipeline"] = "pipeline"
    stages: list[Stage]

class QueryAnswer(BaseModel):
    """Result of the query."""

    query: Pipeline
    meta: QueryAnswerMeta
    rows: list[list[Any]]

    def to_pandas(self) -> pd.DataFrame:
        """Convert the result to a pandas DataFrame."""
        types_map = {}
        for col in self.meta.columns:
            types_map[col.name] = col.type.to_pandas_type()

        return pd.DataFrame(
            self.rows,
            columns=[col.name for col in self.meta.columns],
        ).astype(types_map)


class QueryAnswerMetaColumn(BaseModel):
    """Metadata for a column in the result of the query."""

    name: str
    type: LiteralType


class QueryAnswerMeta(BaseModel):
    """Metadata for the result of the query."""

    columns: list[QueryAnswerMetaColumn]
    tables: Optional[list[Table]] = None
