#!/usr/bin/env python
"""This module contains code that helps generates the necessary queries that
read data from the database"""
from typing import Dict, List, TypeVar, Any, Iterable

from dateutil.parser import parse as parse_date
from sqlalchemy import desc, asc
from sqlalchemy.orm import Session

SessionType = TypeVar('SessionType', bound=Session)
SUPPORTED_QUERY_OPERATORS = (
    '$ne', '$eq', '$in', '$nin', '$gt', '$gte', '$lt', '$lte'
)


def query(
        session_obj: SessionType, model_cls,
        params: List[Dict], pagination_args: Dict):
    """This function is responsible for returning a filter list of model
    instances from the database.

    Arg(s):
    -------
    session_obj (SessionType) -> The object used to interact with the data model
    model_cls -> Model class that represents the database table to get data from
    params (List[Dict]) -> The list of filter conditions that will be used to
        filter the data that is returned
    pagination_args (Dict) -> The pagination arguments that indicates how many
        entities to be returned from the database and how many records to be
        skipped
    Return(s):
    ----------
        returns a list of instance of class passed in the model_cls param
    """
    params = params if params else []
    pagination_args = pagination_args if pagination_args else {}

    record_set = session_obj.query(model_cls)
    for param in params:
        record_set = _apply_query_param(model_cls, record_set, param)
    record_set = _query_sort(
        model_cls, record_set, pagination_args.get("sort", []))
    return query_limit(record_set, pagination_args)


def query_limit(record_set, pagination_args: Dict = None):
    """This function applies the pagination arguments to the record set that is
    passed to it

    Arg(s)
    ------
    record_set -> the record set object that needs the pagination arguments
        applied to it
    pagination_args (Dict) -> the pagination arguments that need to be applied
        to the record set

    Return(s)
    ---------
    record_set -> a new record set with the pagination arguments applied to it
    """
    pagination_args = pagination_args if pagination_args else {}

    if pagination_args.get("offset", 0) > 0:
        record_set = record_set.offset(pagination_args["offset"])

    if pagination_args.get("limit", 0) > 0:
        record_set = record_set.limit(pagination_args["limit"])

    return record_set


def _query_sort(model_cls, record_set, sort_params: List[List]):
    """This function is responsible for applying a sort to a particular record
    set

    Arg(s)
    ------
    model_cls -> class representing the model whose record set we to sort
    record_set -> instance of the record set that the sort must be applied to
    sort_params (List[List]) -> the list of sort that need to be applied to the
        record set

    Return(s)
    ---------
    record_set -> returns a new record set with the sort params applied to it
    """
    for sort_param in sort_params:
        for (field, ordering) in sort_param:
            if str(ordering).upper() == "ASC":
                order_func = asc
            elif str(ordering).upper() == "DESC":
                order_func = desc
            else:
                raise NotImplementedError(
                    "{0} isn't a valid ordering functions".format(ordering))
            record_set.order_by(order_func(getattr(model_cls, field)))
    return record_set


def _convert_if_date(value: Any):
    """This function converts the value passed to it to a date or list of dates
    if it is annotated as containing a date. This is necessary because dates are
    not natively supported in json

    Args(s):
    --------
    value -> the value to be converted to a native date value if it's a date

    Return(s):
    ----------
    returns the same value field if it is not annotated as contains a date or
    it returns a native date or list of native date values
    """
    if hasattr(value, 'items') and hasattr(value, "fromkeys"):
        if hasattr(
                value["$date"], "append") and hasattr(value["$date"], "clear"):
            return [parse_date(date_val) for date_val in value["$date"]]
        return parse_date(value["$date"])
    return value


def _apply_query_param(model_cls, record_set, params: Dict) -> bool:
    """This function is responsible for applying a filter parameter to a
    record set

    Arg(s)
    ------
    model_cls -> class representing the model that the filter parameter must be
        applied
    record_set -> record set instance that the filter parameter must be applied
    params (Dict) -> Dictionary that represents the filtering paramater that
        must be applied

    Return(s)
    ---------
    returns a new record set instance with the filtering parameter applied to it
    """
    for field in params:
        for operator in params[field].keys():
            operator_value = _convert_if_date(params[field][operator])
            if operator == "$eq":

                if operator_value is None:
                    return record_set.filter(
                        getattr(model_cls, field).is_(None))

                return record_set.filter(
                    getattr(model_cls, field) == operator_value)

            elif operator == "$ne":
                return record_set.filter(
                    getattr(model_cls, field) != operator_value)
            elif operator == "$lt":
                return record_set.filter(
                    getattr(model_cls, field) < operator_value)
            elif operator == "$lte":
                return record_set.filter(
                    getattr(model_cls, field) < operator_value |
                    getattr(model_cls, field) == operator_value
                )
            elif operator == "$gt":
                return record_set.filter(
                    getattr(model_cls, field) > operator_value)
            elif operator == "$gte":
                return record_set.filter(
                    getattr(model_cls, field) > operator_value |
                    getattr(model_cls, field) == operator_value
                )
            elif operator == "$nin":
                return record_set.filter(
                    getattr(model_cls, field).notin_(operator_value))
            elif operator == "$in":
                return record_set.filter(
                    getattr(model_cls, field).in_(operator_value))


def count(session_obj: SessionType, model_cls, params: List[Dict]=None):
    """This function is responsible for returning the number of instances of a 
    model match a list of filter parameters

    Arg(s):
    ------
    session_obj -> object used to interact with the database
    model_cls -> model classes whose instances we want to count
    params (List[Dict]) -> parameter to used to filter the instances to be
        counted

    Return(s):
    ----------
    dictionary representing the count of the instances that matched params
    """
    result = query(session_obj, model_cls, params, {})

    count_ = 0
    if result:
        count_ = result.count()

    return {"count": count_}
