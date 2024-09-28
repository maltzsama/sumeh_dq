#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cuallee import Check, CheckLevel
from .configs.config_parser import get_config


def quality(df, file_path, kind, name="Data Quality Checks", delimiter=";"):

    dq_rules = get_config(kind=kind, file_path=file_path, delimiter=delimiter)

    check = Check(CheckLevel.WARNING, name)
    print(dq_rules)
    for rule in dq_rules:
        rule_name = rule["check_type"]
        field = rule["field"]
        threshold = rule.get("threshold", 1.0)
        threshold = 1.0 if threshold is None else threshold

        print(rule_name)
        print(field)
        print(type(field))
        if rule_name == "is_unique":
            check = check.is_unique(field, pct=threshold)

        elif rule_name == "is_complete":
            check = check.is_complete(field, pct=threshold)

        elif rule_name in ("is_contained_in", "is_in"):
            values = rule["value"]
            values = values.replace("[", "").replace("]", "").split(",")
            values = tuple([value.strip() for value in values])
            check = check.is_contained_in(field, values, pct=threshold)

        elif rule_name in ("not_contained_in", "not_in"):
            values = rule["value"]
            values = values.replace("[", "").replace("]", "").split(",")
            values = tuple([value.strip() for value in values])
            check = check.is_contained_in(field, values, pct=threshold)

        elif rule_name == "is_positive":
            check = check.is_positive(field, pct=threshold)

        elif rule_name == "has_pattern":
            pattern = rule["value"]
            check = check.has_pattern(field, pattern, pct=threshold)

        elif rule_name == "are_unique":

            check = check.are_unique(field, pct=threshold)

        elif rule_name == "is_greater_than":
            value = rule["value"]
            check = check.is_greater_than(field, value, pct=threshold)

        elif rule_name == "is_greater_or_equal_than":
            value = rule["value"]
            check = check.is_greater_or_equal_than(field, value, pct=threshold)

        elif rule_name == "satisfies":
            predicate = rule["predicate"]
            check = check.satisfies(field, predicate, pct=threshold)

    quality_check = check.validate(df)
    return quality_check
