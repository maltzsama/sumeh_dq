"""Main module."""
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import List, Dict
from cuallee import Check, CheckLevel
from configs.config_parser import read_csv_file

def quality(df, data_quality_rules):
    """
    Apply data quality checks to a DataFrame based on the provided rules.

    Args:
        df (DataFrame): The DataFrame to be checked for data quality.
        data_quality_rules (List[Dict]): A list of data quality rules to apply. Each rule should be a dictionary
                                         containing the rule name and associated parameters.

    Returns:
        DataFrame: A DataFrame containing the results of the data quality checks.
    """
    check = Check(CheckLevel.WARNING, "Data Quality Checks")

    for rule in data_quality_rules:
        rule_name = rule["name"]
        for field_rule in rule["fields"]:
            field = field_rule["field"]
            threshold = field_rule.get("threshold", 1.0)

            if rule_name == "is_unique":
                check = check.is_unique(field, pct=threshold)

            elif rule_name == "is_complete":
                check = check.is_complete(field, pct=threshold)

            elif rule_name == "is_contained_in":
                values = field_rule["in"]
                check = check.is_contained_in(field, values, pct=threshold)

            elif rule_name == "is_positive":
                check = check.is_positive(field, pct=threshold)

            elif rule_name == "has_pattern":
                pattern = field_rule["pattern"]
                check = check.has_pattern(field, pattern, pct=threshold)

            elif rule_name == "are_unique":
                sub_fields = field_rule["sub_fields"]
                check = check.are_unique(sub_fields, pct=threshold)

            elif rule_name == "is_greater_than":
                value = field_rule["value"]
                check = check.is_greater_than(field, value, pct=threshold)

            elif rule_name == "is_greater_or_equal_than":
                value = field_rule["value"]
                check = check.is_greater_or_equal_than(field, value, pct=threshold)

            elif rule_name == "satisfies":
                predicate = field_rule["predicate"]
                check = check.satisfies(field, predicate, pct=threshold)

    quality_check = check.validate(df)
    return quality_check
