from typing import Generic, List, TypeVar

from venom import Message
from venom.common import FieldMask
from venom.common.types import JSONObject, JSONValue
from venom.fields import String, Integer, Field, RepeatField

E = TypeVar('E')


class ListEntitiesRequest(Message):
    filters = Field(JSONObject)
    order = RepeatField(JSONValue)

    page_token = String()
    page_size = Integer()


class ListEntitiesResponse(Message):
    next_page_token = String()
    items = RepeatField(Message)


class UpdateEntityRequest(Message):
    update_mask = Field(FieldMask)
