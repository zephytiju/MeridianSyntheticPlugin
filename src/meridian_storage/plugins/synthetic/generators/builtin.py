# SPDX-License-Identifier: Apache-2.0
"""Built-in generators covering every Meridian V1 logical type."""

from __future__ import annotations

import base64
import math
import re
import string
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, Decimal
from typing import cast

from meridian_storage.semantics import LogicalKind, ResourceReference

from ..canonical import JsonValue, deterministic_uuid, sha256_digest
from ..errors import InvalidSpec
from .base import (
    GenerationContext,
    GeneratorDefinition,
    GeneratorRegistry,
    ValueGenerator,
    exact_config,
)

_ALL_KINDS = frozenset(LogicalKind)
_NUMERIC_KINDS = frozenset(
    {
        LogicalKind.INT8,
        LogicalKind.INT16,
        LogicalKind.INT32,
        LogicalKind.INT64,
        LogicalKind.DECIMAL,
        LogicalKind.FLOAT64,
    }
)
_NAMES: Mapping[str, tuple[str, ...]] = {
    "en": ("Avery", "Jordan", "Morgan", "Riley", "Taylor", "Casey"),
    "zh": ("晨曦", "嘉宁", "思远", "雨桐", "子墨", "若溪"),
}


def _digest(generator_id: str) -> str:
    return sha256_digest(
        cast(JsonValue, {"generator": generator_id, "algorithm": "meridian-synthetic-v1"})
    )


def _number(value: JsonValue | None, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("numeric generator option must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("numeric generator option must be finite")
    return result


def _integer_bounds(context: GenerationContext, config: Mapping[str, JsonValue]) -> tuple[int, int]:
    bits = {
        LogicalKind.INT8: 8,
        LogicalKind.INT16: 16,
        LogicalKind.INT32: 32,
        LogicalKind.INT64: 64,
    }[context.field.logical_type.kind]
    lower = -(2 ** (bits - 1))
    upper = 2 ** (bits - 1) - 1
    constraints = context.field.constraints
    if "min" in constraints:
        lower = max(lower, math.ceil(float(cast(str | int | float, constraints["min"]))))
    if "exclusiveMin" in constraints:
        lower = max(
            lower,
            math.floor(float(cast(str | int | float, constraints["exclusiveMin"]))) + 1,
        )
    if "max" in constraints:
        upper = min(upper, math.floor(float(cast(str | int | float, constraints["max"]))))
    if "exclusiveMax" in constraints:
        upper = min(
            upper,
            math.ceil(float(cast(str | int | float, constraints["exclusiveMax"]))) - 1,
        )
    lower = int(_number(config.get("min"), lower))
    upper = int(_number(config.get("max"), upper))
    if lower > upper:
        raise ValueError("integer generator minimum exceeds maximum")
    return lower, upper


def _constant(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    del context
    return config["value"]


def _sequence(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    start = _number(config.get("start"), 0)
    step = _number(config.get("step"), 1)
    value = start + step * context.ordinal
    if context.field.logical_type.kind in {
        LogicalKind.INT8,
        LogicalKind.INT16,
        LogicalKind.INT32,
        LogicalKind.INT64,
    }:
        return int(value)
    if context.field.logical_type.kind is LogicalKind.DECIMAL:
        scale = cast(int, context.field.logical_type.scale)
        return f"{Decimal(str(value)):.{scale}f}"
    return float(value)


def _integer(context: GenerationContext, config: Mapping[str, JsonValue]) -> int:
    lower, upper = _integer_bounds(context, config)
    if context.identity_field:
        if "min" not in config and not {"min", "exclusiveMin"} & set(context.field.constraints):
            lower = max(lower, 0)
        candidate = lower + context.ordinal
        if candidate > upper:
            raise InvalidSpec(
                f"identity field {context.field.name!r} cannot represent every ordinal"
            )
        return candidate
    return context.rng().randint(lower, upper)


def _float(context: GenerationContext, config: Mapping[str, JsonValue]) -> float:
    lower = _number(config.get("min"), -1_000_000.0)
    upper = _number(config.get("max"), 1_000_000.0)
    constraints = context.field.constraints
    for key in ("min", "exclusiveMin"):
        if key in constraints:
            lower = max(lower, float(cast(str | int | float, constraints[key])))
    for key in ("max", "exclusiveMax"):
        if key in constraints:
            upper = min(upper, float(cast(str | int | float, constraints[key])))
    if lower > upper:
        raise ValueError("float generator minimum exceeds maximum")
    distribution = config.get("distribution", "uniform")
    rng = context.rng()
    if distribution == "normal":
        mean = _number(config.get("mean"), (lower + upper) / 2)
        deviation = _number(config.get("deviation"), max((upper - lower) / 6, 1e-12))
        return min(upper, max(lower, rng.gauss(mean, deviation)))
    if distribution != "uniform":
        raise ValueError("float distribution must be uniform or normal")
    return rng.uniform(lower, upper)


def _decimal(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    scale = cast(int, context.field.logical_type.scale)
    precision = cast(int, context.field.logical_type.precision)
    limit = Decimal(10) ** (precision - scale) - (Decimal(10) ** -scale)
    lower = Decimal(str(config.get("min", -limit)))
    upper = Decimal(str(config.get("max", limit)))
    for key in ("min", "exclusiveMin"):
        if key in context.field.constraints:
            lower = max(lower, Decimal(str(context.field.constraints[key])))
    for key in ("max", "exclusiveMax"):
        if key in context.field.constraints:
            upper = min(upper, Decimal(str(context.field.constraints[key])))
    if lower > upper:
        raise ValueError("decimal generator minimum exceeds maximum")
    fraction = Decimal(str(context.rng().random()))
    quantum = Decimal(1).scaleb(-scale)
    value = (lower + ((upper - lower) * fraction)).quantize(quantum, rounding=ROUND_DOWN)
    return format(value, "f")


def _boolean(context: GenerationContext, config: Mapping[str, JsonValue]) -> bool:
    probability = _number(config.get("probability"), 0.5)
    if not 0 <= probability <= 1:
        raise ValueError("boolean probability must be between zero and one")
    return context.rng().random() < probability


def _string_lengths(context: GenerationContext, config: Mapping[str, JsonValue]) -> tuple[int, int]:
    constraints = context.field.constraints
    minimum = int(cast(int, constraints.get("minLength", 1)))
    maximum = int(cast(int, constraints.get("maxLength", max(16, minimum))))
    minimum = int(_number(config.get("minLength"), minimum))
    maximum = int(_number(config.get("maxLength"), maximum))
    if minimum < 0 or maximum < minimum or maximum > 1_000_000:
        raise ValueError("string length bounds are invalid")
    return minimum, maximum


def _string_value(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    minimum, maximum = _string_lengths(context, config)
    prefix_value = config.get("prefix", "")
    charset_value = config.get("charset", string.ascii_letters + string.digits)
    if not isinstance(prefix_value, str) or not isinstance(charset_value, str) or not charset_value:
        raise TypeError("string prefix and charset must be strings")
    if context.identity_field:
        value = f"{prefix_value}{context.collection_id}-{context.ordinal:012d}"
        if not minimum <= len(value) <= maximum:
            raise ValueError("identity string cannot fit configured length bounds")
        return value
    rng = context.rng()
    pattern = context.field.constraints.get("pattern")
    for attempt in range(256):
        length = rng.randint(max(minimum, len(prefix_value)), maximum)
        value = prefix_value + "".join(
            rng.choice(charset_value) for _ in range(length - len(prefix_value))
        )
        if pattern is None or re.search(cast(str, pattern), value) is not None:
            return value
        rng = context.rng(f"pattern-{attempt}")
    raise ValueError("bounded string generation could not satisfy the declared pattern")


def _choice(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    values = config["values"]
    if (
        not isinstance(values, Sequence)
        or isinstance(values, str | bytes | bytearray)
        or not values
    ):
        raise TypeError("choice values must be a non-empty array")
    weights = config.get("weights")
    if weights is None:
        return context.rng().choice(tuple(values))
    if not isinstance(weights, Sequence) or isinstance(weights, str | bytes | bytearray):
        raise TypeError("choice weights must be an array")
    if len(weights) != len(values) or any(
        isinstance(item, bool) or not isinstance(item, int | float) or item < 0 for item in weights
    ):
        raise ValueError("choice weights must match values and be non-negative")
    return context.rng().choices(tuple(values), weights=cast(Sequence[float], weights), k=1)[0]


def _uuid(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    del config
    return deterministic_uuid(
        context.spec_seed,
        context.collection_id,
        context.field.name,
        context.ordinal,
        context.element_index,
    )


def _interval(context: GenerationContext) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(context.time_start.replace("Z", "+00:00")).astimezone(UTC)
    end = datetime.fromisoformat(context.time_end.replace("Z", "+00:00")).astimezone(UTC)
    return start, end


def _timestamp(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    del config
    start, end = _interval(context)
    seconds = (end - start).total_seconds() * context.rng().random()
    return (
        (start + timedelta(seconds=seconds))
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def _date(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    del config
    start, end = _interval(context)
    span = max(0, (end.date() - start.date()).days)
    return (start.date() + timedelta(days=context.rng().randint(0, span))).isoformat()


def _duration(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    minimum = int(_number(config.get("minSeconds"), 0))
    maximum = int(_number(config.get("maxSeconds"), 86_400))
    if minimum < 0 or maximum < minimum:
        raise ValueError("duration bounds are invalid")
    return f"PT{context.rng().randint(minimum, maximum)}S"


def _bytes(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    length = int(_number(config.get("length"), 16))
    if not 0 <= length <= 1_048_576:
        raise ValueError("byte length is out of bounds")
    rng = context.rng()
    payload = bytes(rng.getrandbits(8) for _ in range(length))
    return base64.urlsafe_b64encode(payload).rstrip(b"=").decode("ascii")


def _json(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    template = config.get("template")
    if template is not None:
        return template
    return {"ordinal": context.ordinal, "locale": context.locale}


def _enum(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    del config
    return context.rng().choice(context.field.logical_type.enum_values)


def _wgs84(context: GenerationContext, config: Mapping[str, JsonValue]) -> dict[str, float]:
    minimum_longitude = _number(config.get("minLongitude"), -180)
    maximum_longitude = _number(config.get("maxLongitude"), 180)
    minimum_latitude = _number(config.get("minLatitude"), -90)
    maximum_latitude = _number(config.get("maxLatitude"), 90)
    if not (
        -180 <= minimum_longitude <= maximum_longitude <= 180
        and -90 <= minimum_latitude <= maximum_latitude <= 90
    ):
        raise ValueError("WGS84 bounds are invalid")
    rng = context.rng()
    return {
        "longitude": rng.uniform(minimum_longitude, maximum_longitude),
        "latitude": rng.uniform(minimum_latitude, maximum_latitude),
    }


def _record_ref(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    selected = config.get("collection")
    pools = context.references
    if selected is None:
        allowed = context.field.constraints.get("allowedCollections", ())
        for raw in cast(Sequence[object], allowed):
            if isinstance(raw, Mapping):
                canonical = ResourceReference.parse(raw, catalog="structured").canonical
                if canonical in pools:
                    selected = canonical
                    break
    if not isinstance(selected, str) or selected not in pools or not pools[selected]:
        raise ValueError("RecordRef generator requires a populated approved Collection pool")
    pool = pools[selected]
    return pool[context.rng().randrange(len(pool))]


def _object_ref(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    resource = config.get("resource")
    if resource is None:
        resource_ref = ResourceReference.parse(
            {
                "catalog": "object",
                "namespace": "synthetic",
                "name": "fixtures",
            }
        )
    elif isinstance(resource, Mapping | str):
        resource_ref = ResourceReference.parse(resource, catalog="object")
    else:
        raise TypeError("ObjectRef resource must be a logical Resource reference")
    fixture_id = deterministic_uuid(context.spec_seed, "object-fixture")
    object_id = f"synthetic/{fixture_id}/{context.collection_id}/{context.ordinal}"
    return {"resourceRef": resource_ref.to_dict(), "objectId": object_id}


def _localized_name(context: GenerationContext, config: Mapping[str, JsonValue]) -> str:
    del config
    language = context.locale.split("-", 1)[0].lower()
    names = _NAMES.get(language, _NAMES["en"])
    return context.rng().choice(names)


def _source_field(context: GenerationContext, config: Mapping[str, JsonValue]) -> object:
    field_name = config.get("field")
    if not isinstance(field_name, str) or field_name not in context.source:
        raise ValueError("source-field generator requires an approved projected field")
    return context.source[field_name]


def _default_id(kind: LogicalKind) -> str:
    return {
        LogicalKind.BOOLEAN: "core.boolean@1",
        LogicalKind.INT8: "core.integer@1",
        LogicalKind.INT16: "core.integer@1",
        LogicalKind.INT32: "core.integer@1",
        LogicalKind.INT64: "core.integer@1",
        LogicalKind.DECIMAL: "core.decimal@1",
        LogicalKind.FLOAT64: "core.float@1",
        LogicalKind.STRING: "core.string@1",
        LogicalKind.BYTES: "core.bytes@1",
        LogicalKind.UUID: "core.uuid@1",
        LogicalKind.UTC_TIMESTAMP: "core.timestamp@1",
        LogicalKind.DATE: "core.date@1",
        LogicalKind.DURATION: "core.duration@1",
        LogicalKind.ENUM: "core.enum@1",
        LogicalKind.JSON: "core.json@1",
        LogicalKind.RECORD_REF: "core.record-ref@1",
        LogicalKind.OBJECT_REF: "core.object-ref@1",
        LogicalKind.WGS84_POINT: "core.wgs84@1",
    }[kind]


def default_generator_id(kind: LogicalKind) -> str:
    return _default_id(kind)


def builtin_registry() -> GeneratorRegistry:
    registry = GeneratorRegistry()
    definitions: tuple[
        tuple[
            str,
            frozenset[LogicalKind],
            ValueGenerator,
            frozenset[str],
            frozenset[str],
        ],
        ...,
    ] = (
        ("core.constant@1", _ALL_KINDS, _constant, frozenset({"value"}), frozenset()),
        ("core.sequence@1", _NUMERIC_KINDS, _sequence, frozenset(), frozenset({"start", "step"})),
        (
            "core.integer@1",
            frozenset({LogicalKind.INT8, LogicalKind.INT16, LogicalKind.INT32, LogicalKind.INT64}),
            _integer,
            frozenset(),
            frozenset({"min", "max"}),
        ),
        (
            "core.float@1",
            frozenset({LogicalKind.FLOAT64}),
            _float,
            frozenset(),
            frozenset({"min", "max", "distribution", "mean", "deviation"}),
        ),
        (
            "core.decimal@1",
            frozenset({LogicalKind.DECIMAL}),
            _decimal,
            frozenset(),
            frozenset({"min", "max"}),
        ),
        (
            "core.boolean@1",
            frozenset({LogicalKind.BOOLEAN}),
            _boolean,
            frozenset(),
            frozenset({"probability"}),
        ),
        (
            "core.string@1",
            frozenset({LogicalKind.STRING}),
            _string_value,
            frozenset(),
            frozenset({"minLength", "maxLength", "prefix", "charset"}),
        ),
        ("core.choice@1", _ALL_KINDS, _choice, frozenset({"values"}), frozenset({"weights"})),
        ("core.uuid@1", frozenset({LogicalKind.UUID}), _uuid, frozenset(), frozenset()),
        (
            "core.timestamp@1",
            frozenset({LogicalKind.UTC_TIMESTAMP}),
            _timestamp,
            frozenset(),
            frozenset(),
        ),
        ("core.date@1", frozenset({LogicalKind.DATE}), _date, frozenset(), frozenset()),
        (
            "core.duration@1",
            frozenset({LogicalKind.DURATION}),
            _duration,
            frozenset(),
            frozenset({"minSeconds", "maxSeconds"}),
        ),
        (
            "core.bytes@1",
            frozenset({LogicalKind.BYTES}),
            _bytes,
            frozenset(),
            frozenset({"length"}),
        ),
        (
            "core.json@1",
            frozenset({LogicalKind.JSON}),
            _json,
            frozenset(),
            frozenset({"template"}),
        ),
        ("core.enum@1", frozenset({LogicalKind.ENUM}), _enum, frozenset(), frozenset()),
        (
            "core.wgs84@1",
            frozenset({LogicalKind.WGS84_POINT}),
            _wgs84,
            frozenset(),
            frozenset({"minLongitude", "maxLongitude", "minLatitude", "maxLatitude"}),
        ),
        (
            "core.record-ref@1",
            frozenset({LogicalKind.RECORD_REF}),
            _record_ref,
            frozenset(),
            frozenset({"collection"}),
        ),
        (
            "core.object-ref@1",
            frozenset({LogicalKind.OBJECT_REF}),
            _object_ref,
            frozenset(),
            frozenset({"resource"}),
        ),
        (
            "core.localized-name@1",
            frozenset({LogicalKind.STRING}),
            _localized_name,
            frozenset(),
            frozenset(),
        ),
        (
            "core.source-field@1",
            _ALL_KINDS,
            _source_field,
            frozenset({"field"}),
            frozenset(),
        ),
    )
    for generator_id, kinds, implementation, required, optional in definitions:
        registry.register(
            GeneratorDefinition(
                generator_id=generator_id,
                implementation_digest=_digest(generator_id),
                logical_kinds=kinds,
                generate=implementation,
                config_schema={
                    "formatVersion": "meridian.synthetic.generator-config.v1",
                    "type": "object",
                    "required": sorted(required),
                    "properties": {name: {} for name in sorted(required | optional)},
                    "additionalProperties": False,
                },
                validate_config=exact_config(required=required, optional=optional),
                locales=(
                    frozenset({"en", "en-US", "en-GB", "zh", "zh-CN", "zh-TW"})
                    if generator_id == "core.localized-name@1"
                    else frozenset({"*"})
                ),
            )
        )
    return registry.seal()


__all__ = ["builtin_registry", "default_generator_id"]
