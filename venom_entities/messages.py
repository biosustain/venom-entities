from typing import Generic, List, TypeVar

from venom import Message
from venom.fields import String, Integer

E = TypeVar('E')


class ListEntitiesRequest(Message):
    page_token = String()
    page_size = Integer()


class ListEntitiesResponse(Message):
    next_page_token = String()


class ListEntitiesResult(Generic[E]):
    def __init__(self,
                 items: List[E],
                 next_page_token: str = None):
        self.items = items
        self.next_page_token = next_page_token
