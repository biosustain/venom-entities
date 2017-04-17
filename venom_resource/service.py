from operator import attrgetter

from typing import ClassVar, Any
from venom import Message, Empty
from venom.rpc import Service, http
from venom.rpc.inspection import dynamic

from venom_resource import SQLAlchemyResource
from .resource import Resource


class ResourceService(Service):
    __resources__: ClassVar[SQLAlchemyResource] = set()

    class Meta:
        default_page_size: int = None
        maximum_page_size: int = 100


class DynamicResourceService(ResourceService):
    __resource__: ClassVar[Resource] = Resource(Empty, Empty)

    class Meta:
        pass  # TODO create __resource__ from meta object

    @http.POST('.',
               name=lambda owner: f'create_{owner.__resource__.model_name}',
               http_status=201,
               auto=True)
    @dynamic('request', attrgetter('__resource__.model_message'))
    @dynamic('return', attrgetter('__resource__.model'))
    def create(self, request: Any) -> Any:
        return self.__resource__.create(request)

    @http.GET(attrgetter('__resource__.request_path'),
              name=lambda owner: f'get_{owner.__resource__.model_name}',
              auto=True)
    @dynamic('request', attrgetter('__resource__.get_request_message'))
    @dynamic('return', attrgetter('__resource__.model'))
    def get(self, request: Message) -> Any:
        return self.__resource__.get(request.get(self.__resource__.request_id_field_name))

    @http.POST('.',
               name=lambda owner: f'list_{owner.__resource__.model_plural_name}',
               auto=True)
    @dynamic('request', attrgetter('__resource__.list_request_message'))
    @dynamic('return', attrgetter('__resource__.list_response_message'))
    def list(self, request: Any) -> Any:
        items, next_page_token = self.__resource__.paginate(request)
        return self.__resource__.list_response_message(next_page_token,
                                                       [self.__resource__.format(item) for item in items])

    @http.PATCH(attrgetter('__resource__.request_path'),
                name=lambda owner: f'update_{owner.__resource__.model_name}',
                auto=True)
    @dynamic('request', attrgetter('__resource__.update_request_message'))
    @dynamic('return', attrgetter('__resource__.model'))
    def update(self, request: Any) -> Any:
        entity = self.__resource__.get(request.get(self.__resource__.request_id_field_name))
        return self.__resource__.update(entity, request.get(self.__resource__.model_name), request.update_mask)

    @http.DELETE(attrgetter('__resource__.request_path'),
                 name=lambda owner: f'delete_{owner.__resource__.model_name}',
                 http_status=204,
                 auto=True)
    @dynamic('request', attrgetter('__resource__.get_request_message'))
    def delete(self, request: Message) -> None:
        entity = self.__resource__.get(request.get(self.__resource__.request_id_field_name))
        self.__resource__.delete(entity)
