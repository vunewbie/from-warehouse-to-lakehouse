import attr
from models.base import GeneralDagsConfigs, GCPConfigs
from typing import List
from datetime import timedelta, datetime


@attr.s
class SourceConfigs:
    source_type: str = attr.ib(kw_only=True)
    source_conn_id: str = attr.ib(kw_only=True)
    source_database: str = attr.ib(kw_only=True)
    source_schema: str = attr.ib(kw_only=True, default=None)


@attr.s
class TableConfigs:
    is_disabled: bool = attr.ib(kw_only=True)
    look_back: timedelta = attr.ib(kw_only=True)
    table_name: str = attr.ib(kw_only=True)
    primary_keys: List[str] = attr.ib(kw_only=True)
    clustered_by: List[str] = attr.ib(kw_only=True)
    extract_conditions: List[List[str]] = attr.ib(kw_only=True)
    start_date: datetime = attr.ib(kw_only=True)


@attr.s
class ELTModel(
    GeneralDagsConfigs,
    SourceConfigs,
    GCPConfigs,
    TableConfigs,
):
    pass
