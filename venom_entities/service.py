from operator import attrgetter

from typing import ClassVar, Any, Type
from venom import Message, Empty
from venom.common import FieldMask, Field, Repeat
from venom.common.types import JSONObject, JSONValue
from venom.message import message_factory

from venom.rpc import Service, http
from venom.rpc.inspection import dynamic
from venom.util import upper_camelcase

from venom_entities.messages import ListEntitiesRequest, ListEntitiesResponse
from .resource import Resource
from venom_entities import SQLAlchemyResource


class ResourceService(Service):
    __resources__: ClassVar[SQLAlchemyResource] = set()

    class Meta:
        default_page_size: int = None
        maximum_page_size: int = 100


class DynamicResourceService(ResourceService):
    __resource__: ClassVar[Resource] = Resource(Empty, Empty)

    class Meta:
        pass  # TODO create __resource__ from meta object

    @http.POST('.', http_status=201, auto=True)
    @dynamic('request', attrgetter('__resource__.model_message'))
    @dynamic('return', attrgetter('__resource__.model'))
    def create(self, request: Any) -> Any:
        return self.__resource__.create(request)

    @http.GET('./{id}', auto=True)
    @dynamic('id', attrgetter('__resource__.model_id_type'))
    @dynamic('return', attrgetter('__resource__.model'))
    def get(self, id: Any) -> Any:
        return self.__resource__.get(id)

    @http.POST('.', auto=True)
    @dynamic('request', attrgetter('__resource__.list_request_message'))
    @dynamic('return', attrgetter('__resource__.list_response_message'))
    def list(self, request: Any) -> Any:
        items, next_page_token = self.__resource__.paginate(request)
        return self.__resource__.list_response_message(next_page_token,
                                                       [self.__resource__.format(item) for item in items])

    @http.PATCH('./{id}', auto=True)
    @dynamic('id', attrgetter('__resource__.model_id_type'))
    @dynamic('changes', attrgetter('__resource__.model_message'))
    @dynamic('return', attrgetter('__resource__.model'))
    def update(self, id: Any, changes: Message, update_mask: FieldMask) -> Any:
        entity = self.__resource__.get(id)
        return self.__resource__.update(entity, changes, update_mask)

    @http.DELETE('./{id}', http_status=204, auto=True)
    @dynamic('id', attrgetter('__resource__.model_id_type'))
    def delete(self, id: Any) -> None:
        entity = self.__resource__.get(id)
        self.__resource__.delete(entity)
