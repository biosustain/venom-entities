from typing import ClassVar, TypeVar, Generic

from sqlalchemy import Integer
from venom.common import FieldMask, Message, String, Repeat
from venom.fields import Field
from venom.rpc import http

from venom_entities import EntityResource
from venom_entities.messages import ListEntitiesRequest
from venom_entities.service import ResourceService


def generic(method_decorator):
    return method_decorator


E = TypeVar('E', bound=Message)
E_id = TypeVar('E_id')
M = TypeVar('M')


class ListEntitiesRequest(Message):
    page_token = String()
    page_size = Integer()

    filters = generic(Field(Message))
    sort = Repeat(String())


class ListEntitiesResponse(Message):
    next_page_token = String()
    items = generic(Repeat(Message))


class GenericResourceService(ResourceService, Generic[M, E, E_id]):
    resource: ClassVar[EntityResource]
    message: ClassVar[E]
    message_id: ClassVar[E_id]
    model: ClassVar[M]

    @generic(http.GET('.'))
    def list_entities(self, request: ListEntitiesRequest) -> ListEntitiesResponse:
        return self.resource.paginate()

    @generic(http.POST('.'))
    def create_entity(self, properties: M) -> E:
        return self.resource.create(properties)

    @generic(http.GET('./{entity_id}'))
    def get_entity(self, entity_id: E_id) -> E:
        return self.resource.get(entity_id)

    @generic(http.PATCH('.'))
    def update_entity(self, entity_id: E_id, changes: M, update_mask: FieldMask) -> E:
        return self.resource.update(self.get(entity_id), changes, update_mask)

    @generic(http.DELETE('./{entity_id}'))
    def get_entity(self, entity_id: E_id) -> E:
        return self.resource.delete(entity_id)
