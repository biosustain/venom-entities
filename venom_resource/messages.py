from typing import Generic, List, TypeVar

from venom import Message
from venom.common.types import JSONObject, JSONArray, JSONValue
from venom.fields import String, Integer, Field, Repeat

E = TypeVar('E')


class ListEntitiesRequest(Message):
    filters = Field(JSONObject)
    order = Repeat(JSONValue)

    page_token = String()
    page_size = Integer()


class ListEntitiesResponse(Message):
    next_page_token = String()
    items_ = Repeat(Message, name='items')
