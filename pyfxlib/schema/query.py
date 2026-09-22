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
from typing import Any, Literal, Optional, TypeAlias

import pandas as pd
from pydantic import ConfigDict, Field

from pyfxlib.schema.base import PfxCamelCaseModel


@unique
class Operator(StrEnum):
    """Logical operators for combining filter criteria."""

    AND = "and"
    OR = "or"
    NOT = "not"


@unique
class OperationAgg(StrEnum):
    """Operation aggregate class."""

    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT_ALL = "countAll"
    COUNT_NON_NULL = "countNonNull"
    COUNT_DISTINCT_NON_NULL = "countDistinctNonNull"


@unique
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
    CBRT = "cbrt"
    CEIL = "ceil"
    DEGREES = "degrees"
    FACTORIAL = "factorial"
    FLOOR = "floor"
    GREATEST = "greatest"
    LEAST = "least"
    LN = "ln"
    LOG = "log"
    PI = "pi"
    POWER = "power"
    RADIANS = "radians"
    ROUND = "round"
    SIGN = "sign"
    TRUNC = "trunc"
    RANDOM = "random"
    # Spelled snake_case in the DTO switch, unlike every other name there.
    WIDTH_BUCKET = "width_bucket"
    ACOS = "acos"
    ASIN = "asin"
    ATAN = "atan"
    ATAN2 = "atan2"
    COS = "cos"
    SIN = "sin"
    TAN = "tan"
    DATE_ONLY_TODAY = "dateOnlyToday"
    DATE_TIME_NOW = "dateTimeNow"


@unique
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


class FieldRule(PfxCamelCaseModel):
    """FieldRule class for query criteria on a given field."""

    field_name: str
    operator: FilterOperator
    value: Optional[Any] = None
    start: Optional[Any] = None
    end: Optional[Any] = None


class AdvancedCriteria(PfxCamelCaseModel):
    """AdvancedCriteria class for combining multiple FieldRules with logical operators."""

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


@unique
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

        Same as to_py_type, except that date and datetime types are converted to datetime64[ns]
        and nullable dtypes are used, so that NULL values coming back from the query are kept
        as NA instead of raising (INTEGER) or being silently cast (STRING, BOOLEAN).
        """
        if self in [LiteralType.DATE_ONLY, LiteralType.DATE_TIME]:
            return "datetime64[ns]"
        if self == LiteralType.INTEGER:
            return "Int64"
        if self == LiteralType.REAL:
            # float64 already represents NULL as NaN, so it needs no nullable dtype
            return float
        if self == LiteralType.BOOLEAN:
            return "boolean"
        if self == LiteralType.STRING:
            return "string"
        # Leave anything else as-is, so that structured values are not stringified
        return "object"


class CompanyParameterTables(PfxCamelCaseModel):
    """Company parameter tables table."""

    kind: Literal["companyParameterTables"] = "companyParameterTables"


class CompanyParametersRows(PfxCamelCaseModel):
    """Company parameters rows table."""

    kind: Literal["companyParameterRows"] = "companyParameterRows"
    company_parameter_unique_name: str
    target_date: Optional[date] = None


class Customers(PfxCamelCaseModel):
    """Customers table."""

    kind: Literal["customers"] = "customers"


class Sellers(PfxCamelCaseModel):
    """Sellers table."""

    kind: Literal["sellers"] = "sellers"


class ActionItems(PfxCamelCaseModel):
    """Action items table."""

    kind: Literal["actionItems"] = "actionItems"


class Products(PfxCamelCaseModel):
    """Products table."""

    kind: Literal["products"] = "products"


class ProductCompetitions(PfxCamelCaseModel):
    """Product competitions table."""

    kind: Literal["productCompetitions"] = "productCompetitions"


class ProductReferences(PfxCamelCaseModel):
    """Product references table."""

    kind: Literal["productReferences"] = "productReferences"


class ProductBillOfMaterials(PfxCamelCaseModel):
    """Product bill of materials table."""

    kind: Literal["productBillOfMaterials"] = "productBillOfMaterials"


class JobStatusTrackers(PfxCamelCaseModel):
    """Job status trackers table."""

    kind: Literal["jobStatusTrackers"] = "jobStatusTrackers"


class ProductExtensionRows(PfxCamelCaseModel):
    """Product extension rows table."""

    kind: Literal["productExtensionRows"] = "productExtensionRows"
    product_extension_name: str


class CustomerExtensionRows(PfxCamelCaseModel):
    """Customer extension rows table."""

    kind: Literal["customerExtensionRows"] = "customerExtensionRows"
    customer_extension_name: str


class SellerExtensionRows(PfxCamelCaseModel):
    """Seller extension rows table."""

    kind: Literal["sellerExtensionRows"] = "sellerExtensionRows"
    seller_extension_name: str


class ConditionRecords(PfxCamelCaseModel):
    """Condition records table."""

    kind: Literal["conditionRecords"] = "conditionRecords"
    condition_record_set_unique_name: str


class QuoteLineItems(PfxCamelCaseModel):
    """Quote line items table."""

    kind: Literal["quoteLineItems"] = "quoteLineItems"
    quote_unique_name: str


class RebateLineItems(PfxCamelCaseModel):
    """Rebate line items table."""

    kind: Literal["rebateLineItems"] = "rebateLineItems"
    rebate_unique_name: str


class ContractLineItems(PfxCamelCaseModel):
    """Contract line items table."""

    kind: Literal["contractLineItems"] = "contractLineItems"
    contract_unique_name: str


class CompensationLineItems(PfxCamelCaseModel):
    """Compensation line items table."""

    kind: Literal["compensationLineItems"] = "compensationLineItems"
    compensation_unique_name: str


class Claims(PfxCamelCaseModel):
    """Claims table."""

    kind: Literal["claims"] = "claims"


class ClaimLineItems(PfxCamelCaseModel):
    """Claim line items table."""

    kind: Literal["claimLineItems"] = "claimLineItems"
    claim_unique_name: str


class CalculationGrids(PfxCamelCaseModel):
    """Calculation grids table."""

    kind: Literal["calculationGrids"] = "calculationGrids"


class CalculationGridLineItems(PfxCamelCaseModel):
    """Calculation grid line items table."""

    kind: Literal["calculationGridLineItems"] = "calculationGridLineItems"
    calculation_grid_id: int


class PriceLists(PfxCamelCaseModel):
    """Price lists table."""

    kind: Literal["priceLists"] = "priceLists"


class PriceListLineItems(PfxCamelCaseModel):
    """Price list items table."""

    kind: Literal["priceListLineItems"] = "priceListLineItems"
    price_list_id: int


class PriceGrids(PfxCamelCaseModel):
    """Price grids table."""

    kind: Literal["priceGrids"] = "priceGrids"


class PriceGridLineItems(PfxCamelCaseModel):
    """Price grid line items table."""

    kind: Literal["priceGridLineItems"] = "priceGridLineItems"
    price_grid_id: int


class RebateRecords(PfxCamelCaseModel):
    """Rebate records table."""

    kind: Literal["rebateRecords"] = "rebateRecords"
    rebate_record_set_id: int


class CompensationRecords(PfxCamelCaseModel):
    """Compensation records table."""

    kind: Literal["compensationRecords"] = "compensationRecords"
    compensation_record_set_id: int


class PADataSource(PfxCamelCaseModel):
    """Data source table."""

    kind: Literal["datasource"] = "datasource"
    data_source_unique_name: str


class PADatamart(PfxCamelCaseModel):
    """Datamart table."""

    kind: Literal["datamart"] = "datamart"
    datamart_unique_name: str
    currency: Optional[str] = None
    uom: Optional[str] = None


class PADataFeed(PfxCamelCaseModel):
    """Data feed table."""

    kind: Literal["datafeed"] = "datafeed"
    data_feed_unique_name: str


class PAModelTable(PfxCamelCaseModel):
    """PA Model table."""

    kind: Literal["modelTable"] = "modelTable"
    model_unique_name: str
    table_name: str


class PARollup(PfxCamelCaseModel):
    """PA Rollup table."""

    kind: Literal["rollup"] = "rollup"
    rollup_label: str


class CustomForms(PfxCamelCaseModel):
    """Custom forms table."""

    kind: Literal["customForms"] = "customForms"
    custom_form_type_unique_name: str


Table: TypeAlias = (
    CompanyParameterTables
    | CompanyParametersRows
    | Products
    | ProductCompetitions
    | ProductReferences
    | ProductBillOfMaterials
    | Customers
    | Sellers
    | ActionItems
    | JobStatusTrackers
    | ProductExtensionRows
    | CustomerExtensionRows
    | SellerExtensionRows
    | ConditionRecords
    | QuoteLineItems
    | RebateLineItems
    | ContractLineItems
    | CompensationLineItems
    | Claims
    | ClaimLineItems
    | CalculationGrids
    | CalculationGridLineItems
    | PriceLists
    | PriceListLineItems
    | PriceGrids
    | PriceGridLineItems
    | RebateRecords
    | CompensationRecords
    | PADataFeed
    | PADataSource
    | PADatamart
    | PAModelTable
    | PARollup
    | CustomForms
)
# All Selectables


class SourceColumnReference(PfxCamelCaseModel):
    """Source column class."""

    kind: Literal["columnReference"] = "columnReference"
    source: Literal["table"] = "table"
    column: str


class PreviousStageColumnReference(PfxCamelCaseModel):
    """Previous stage column class."""

    kind: Literal["columnReference"] = "columnReference"
    source: Literal["previousStage"] = "previousStage"
    column: str


class PipelineColumnReference(PfxCamelCaseModel):
    """Joined pipeline column class."""

    kind: Literal["columnReference"] = "columnReference"
    source: Literal["pipeline"] = "pipeline"
    column: str


class InputColumnReference(PfxCamelCaseModel):
    """Input column reference."""

    kind: Literal["inputColumnReference"] = "inputColumnReference"
    source: Literal["table"] = "table"
    input_name: str
    column: str


class OutputColumnReference(PfxCamelCaseModel):
    """Output column reference."""

    kind: Literal["outputColumnReference"] = "outputColumnReference"
    source: Literal["table"] = "table"
    element_name: str
    column: str


class CalculationResultColumnReference(PfxCamelCaseModel):
    """Calculation result column reference."""

    kind: Literal["calculationResultColumnReference"] = "calculationResultColumnReference"
    source: Literal["table"] = "table"
    element_name: str


class ActiveCalculationResultColumnReference(PfxCamelCaseModel):
    """Active calculation result column reference."""

    kind: Literal["activeCalculationResultColumnReference"] = (
        "activeCalculationResultColumnReference"
    )
    source: Literal["table"] = "table"
    element_name: str


class PreviousCalculationResultColumnReference(PfxCamelCaseModel):
    """Previous calculation result column reference."""

    kind: Literal["previousCalculationResultColumnReference"] = (
        "previousCalculationResultColumnReference"
    )
    source: Literal["table"] = "table"
    element_name: str


class LiteralValue(PfxCamelCaseModel):
    """Literal class."""

    kind: Literal["literal"] = "literal"
    type: LiteralType
    value: Any


@unique
class WindowOperation(StrEnum):
    """Functions the DTO dispatches through FunctionDTO's window overload.

    A disjoint set from Operation: the window overload rejects every scalar name, and the
    scalar overload rejects the ranking ones.
    """

    AVG = "avg"
    COUNT_ALL = "countAll"
    COUNT_NON_NULL = "countNonNull"
    CUME_DIST = "cumeDist"
    DENSE_RANK = "denseRank"
    FIRST_VALUE = "firstValue"
    LAG = "lag"
    LAST_VALUE = "lastValue"
    LEAD = "lead"
    MAX = "max"
    MIN = "min"
    NTH_VALUE = "nthValue"
    NTILE = "ntile"
    PERCENT_RANK = "percentRank"
    RANK = "rank"
    ROW_NUMBER = "rowNumber"
    SUM = "sum"


@unique
class FrameType(StrEnum):
    """Window frame type class. Lower-case on the wire, unlike the other enums."""

    ROWS = "rows"
    RANGE = "range"
    GROUP = "group"


@unique
class FrameExclusion(StrEnum):
    """Window frame exclusion class."""

    CURRENT_ROW = "CURRENT_ROW"
    GROUP = "GROUP"
    TIES = "TIES"
    NONE = "NONE"


@unique
class BoundDirection(StrEnum):
    """Window frame bound direction class."""

    PRECEDING = "PRECEDING"
    FOLLOWING = "FOLLOWING"


class UnboundedBound(PfxCamelCaseModel):
    """Window frame bound running to the start or end of the partition."""

    type: Literal["unbounded"] = "unbounded"
    direction: BoundDirection


class OffsetBound(PfxCamelCaseModel):
    """Window frame bound at a fixed offset from the current row."""

    type: Literal["offset"] = "offset"
    offset: int
    direction: BoundDirection


class CurrentRowBound(PfxCamelCaseModel):
    """Window frame bound at the current row."""

    type: Literal["currentRow"] = "currentRow"


Bound: TypeAlias = UnboundedBound | OffsetBound | CurrentRowBound


class Frame(PfxCamelCaseModel):
    """Window frame class.

    FrameDTO is a plain record rather than a QueryApiDTO, so unlike every other model here it
    carries no `kind`, and its bounds discriminate on `type`.
    """

    type: FrameType
    start: Bound
    end: Bound
    exclusion: FrameExclusion


class PreviousStageWindowFunctionCall(PfxCamelCaseModel):
    """The function a window function applies over its frame."""

    kind: Literal["function"] = "function"
    name: WindowOperation
    arguments: list["PreviousStageExpression"] = Field(default_factory=list)


class PreviousStageWindowFunction(PfxCamelCaseModel):
    """Window function class."""

    kind: Literal["windowFunction"] = "windowFunction"
    frame: Frame
    partitions: list["PreviousStageExpression"] = Field(default_factory=list)
    filter: Optional["PreviousStageExpression"] = None
    orders: list["Order"] = Field(default_factory=list)
    function: PreviousStageWindowFunctionCall


class PreviousStageFunction(PfxCamelCaseModel):
    """Function class."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["PreviousStageExpression"]


PreviousStageExpression: TypeAlias = (
    LiteralValue
    | PreviousStageColumnReference
    | PreviousStageWindowFunction
    | PreviousStageFunction
)


class SourceFunction(PfxCamelCaseModel):
    """Source Function class."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["SourceExpression"]


# Every expression addressable inside a source stage. The DTO has a single flat
# `ExpressionDTO` hierarchy and `FunctionDTO.arguments` is a plain `List<ExpressionDTO>`,
# so any reference kind nests at any depth. Which of them a given table actually accepts
# is a runtime check server-side (`switch (t) { case InputsColumns c -> ...; default ->
# throw }`), not something the wire format encodes.
SourceExpression: TypeAlias = (
    LiteralValue
    | SourceColumnReference
    | InputColumnReference
    | OutputColumnReference
    | CalculationResultColumnReference
    | ActiveCalculationResultColumnReference
    | PreviousCalculationResultColumnReference
    | SourceFunction
)


class SourceSelectable(PfxCamelCaseModel):
    """Source Selectable class."""

    kind: Literal["selectable"] = "selectable"
    expression: SourceExpression
    alias: str


class PreviousStageSelectable(PfxCamelCaseModel):
    """Selectable class."""

    kind: Literal["selectable"] = "selectable"
    expression: PreviousStageExpression
    alias: str


# Special case for aggregable expressions


class PreviousStageFunctionAgg(PfxCamelCaseModel):
    """Function agg class."""

    kind: Literal["function"] = "function"
    name: OperationAgg
    arguments: list[PreviousStageExpression]


ExpressionAgg: TypeAlias = (
    LiteralValue | PreviousStageColumnReference | PreviousStageFunctionAgg | PreviousStageFunction
)


class PreviousStageSelectableAgg(PfxCamelCaseModel):
    """Selectable agg class."""

    kind: Literal["selectable"] = "selectable"
    expression: ExpressionAgg
    alias: str


# All the stage types


class Source(PfxCamelCaseModel):
    """Source class."""

    kind: Literal["source"] = "source"
    table: Table
    columns: list[SourceSelectable] = Field(default_factory=list)
    criteria: Optional[SourceExpression] = None


@unique
class JoinType(StrEnum):
    """Join type class."""

    INNER = "INNER"
    LEFT_OUTER = "LEFT_OUTER"


class JoinTableFunction(PfxCamelCaseModel):
    """Function class for joins against a table."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["JoinTableExpression"]


JoinTableExpression: TypeAlias = (
    LiteralValue
    | PreviousStageColumnReference
    | SourceColumnReference
    | InputColumnReference
    | OutputColumnReference
    | CalculationResultColumnReference
    | ActiveCalculationResultColumnReference
    | PreviousCalculationResultColumnReference
    | JoinTableFunction
)


class JoinTableSelectable(PfxCamelCaseModel):
    """Selectable class for joins against a table."""

    kind: Literal["selectable"] = "selectable"
    expression: JoinTableExpression
    alias: str


class JoinPipelineFunction(PfxCamelCaseModel):
    """Function class for joins against a pipeline."""

    kind: Literal["function"] = "function"
    name: Operation
    arguments: list["JoinPipelineExpression"]


JoinPipelineExpression: TypeAlias = (
    LiteralValue | PreviousStageColumnReference | PipelineColumnReference | JoinPipelineFunction
)


class JoinPipelineSelectable(PfxCamelCaseModel):
    """Selectable class for joins against a pipeline."""

    kind: Literal["selectable"] = "selectable"
    expression: JoinPipelineExpression
    alias: str


class JoinTable(PfxCamelCaseModel):
    """Join against a table."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["join"] = "join"
    table: Table
    type: JoinType
    columns: list[JoinTableSelectable]
    criteria: JoinTableExpression


class JoinPipeline(PfxCamelCaseModel):
    """Join against another pipeline."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["join"] = "join"
    pipeline: "Pipeline"
    type: JoinType
    columns: list[JoinPipelineSelectable]
    criteria: JoinPipelineExpression


Join: TypeAlias = JoinTable | JoinPipeline


class AddColumns(PfxCamelCaseModel):
    """Add columns class."""

    kind: Literal["addColumns"] = "addColumns"
    columns: list[PreviousStageSelectable] = Field(min_length=1)


class RemoveColumns(PfxCamelCaseModel):
    """Remove columns class."""

    kind: Literal["removeColumns"] = "removeColumns"
    columns: list[str] = Field(min_length=1)


class RetainColumns(PfxCamelCaseModel):
    """Retain columns class."""

    kind: Literal["retainColumns"] = "retainColumns"
    columns: list[str] = Field(min_length=1)


class SelectColumns(PfxCamelCaseModel):
    """Select columns class."""

    kind: Literal["selectColumns"] = "selectColumns"
    columns: list[PreviousStageSelectable] = Field(min_length=1)


class Filter(PfxCamelCaseModel):
    """Filter class."""

    kind: Literal["filter"] = "filter"
    criteria: PreviousStageExpression


class Aggregate(PfxCamelCaseModel):
    """Aggregate class."""

    kind: Literal["aggregate"] = "aggregate"
    columns: list[PreviousStageSelectableAgg] = Field(min_length=1)
    dimensions: list[PreviousStageExpression]


class Distinct(PfxCamelCaseModel):
    """Distinct class."""

    kind: Literal["distinct"] = "distinct"


class Take(PfxCamelCaseModel):
    """Take class."""

    kind: Literal["take"] = "take"
    count: int


@unique
class OrderDirection(StrEnum):
    """Order direction class."""

    ASC_NULLS_FIRST = "ASC_NULLS_FIRST"
    ASC_NULLS_LAST = "ASC_NULLS_LAST"
    DESC_NULLS_FIRST = "DESC_NULLS_FIRST"
    DESC_NULLS_LAST = "DESC_NULLS_LAST"


class Order(PfxCamelCaseModel):
    """Order class."""

    kind: Literal["order"] = "order"
    expression: PreviousStageExpression
    direction: OrderDirection


class Sort(PfxCamelCaseModel):
    """Sort class."""

    kind: Literal["sort"] = "sort"
    orders: list[Order] = Field(min_length=1)


Stage: TypeAlias = (
    Source
    | Join
    | AddColumns
    | RemoveColumns
    | RetainColumns
    | SelectColumns
    | Filter
    | Aggregate
    | Distinct
    | Take
    | Sort
)

###


class Pipeline(PfxCamelCaseModel):
    """Pipeline class."""

    kind: Literal["pipeline"] = "pipeline"
    stages: list[Stage] = Field(min_length=1)


class QueryAnswer(PfxCamelCaseModel):
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


class QueryAnswerMetaColumn(PfxCamelCaseModel):
    """Metadata for a column in the result of the query."""

    name: str
    type: LiteralType


class QueryAnswerMeta(PfxCamelCaseModel):
    """Metadata for the result of the query."""

    columns: list[QueryAnswerMetaColumn]
    tables: Optional[list[Table]] = None
