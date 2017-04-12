from typing import Generic, Type, Dict, Any, Mapping, Union, TypeVar, NamedTuple, List, Tuple
from venom.common import FieldMask, Message, Converter, Field, Repeat
from venom.common.types import JSONObject, JSONValue
from venom.message import from_object, message_factory
from venom.rpc import Service
from venom.rpc.method import MethodDecorator, HTTPMethodDecorator
from venom.rpc.resolver import Resolver
from venom.util import cached_property, upper_camelcase

from .messages import ListEntitiesRequest, ListEntitiesResponse
from .methods import EntityMethodDescriptor

_Mo = TypeVar('Mo')
_Mo_id = TypeVar('Mo_id')
_M = TypeVar('M', bound=Message)


class _Relationship(NamedTuple):
    reference: Union['Resource', str]
    name: str
    field_name: str


class Relationship(_Relationship):
    @property
    def resource(self) -> 'Resource':
        return Resource.resolve(self.reference)


class Resource(Generic[_Mo, _Mo_id, _M]):
    _resources: Dict[str, 'Resource'] = {}

    name: str = None

    model: Type[_Mo]
    model_name: str = None

    model_message: Type[_M]
    model_id_type: Type[_Mo_id] = int

    order_schema: Any = None
    filter_schema: Any = None

    def __init__(self,
                 model: Type[_Mo],
                 model_message: Type[_M],
                 *,
                 name: str = None,
                 model_name: str = None) -> None:
        self.model = model
        self.model_message = model_message

        if name:
            self.name = name
            self._resources[name] = self

        self.model_name = model_name

    def __set_name__(self, owner, name):
        if not self.name:
            self.name = name
            self._resources[name] = self

    def create(self, properties: Mapping[str, Any]) -> _Mo:
        raise NotImplementedError

    def get(self, id_: _Mo_id, *filters: Any) -> _Mo:
        raise NotImplementedError

    def update(self, entity: _Mo, changes: Mapping[str, Any], mask: FieldMask) -> _Mo:
        raise NotImplementedError

    def paginate(self,
                 page_token: str = '',
                 page_size: int = 0,
                 *filters: Any) -> Tuple[List[_M], str]:
        raise NotImplementedError

    def delete(self, entity: _Mo) -> None:
        raise NotImplementedError

    @cached_property
    def list_request_message(self) -> Type[ListEntitiesRequest]:
        return message_factory(f'List{upper_camelcase(self.name)}Request', {
            'filters': Field(JSONObject, schema=self.filter_schema),
            'order': Repeat(Field(JSONValue, schema=self.order_schema))
        }, super_message=ListEntitiesRequest)

    @cached_property
    def list_response_message(self) -> Type[ListEntitiesResponse]:
        return message_factory(f'List{upper_camelcase(self.name)}Response', {
            'items_': Repeat(self.model_message, name='items')
        }, super_message=ListEntitiesResponse)

    @cached_property
    def entity_converter(self) -> 'ResourceEntityConverter':
        return ResourceEntityConverter(self.model_message, self)

    @cached_property
    def entity_resolver(self) -> 'ResourceEntityResolver':
        return ResourceEntityResolver(self)

    @cached_property
    def rpc(self) -> MethodDecorator:
        return MethodDecorator(EntityMethodDescriptor, resource=self)

    @cached_property
    def http(self) -> HTTPMethodDecorator:
        return HTTPMethodDecorator(EntityMethodDescriptor, resource=self)

    @classmethod
    def resolve(cls, resource_or_resource_name: Union['Resource', str]):
        if isinstance(resource_or_resource_name, Resource):
            return resource_or_resource_name
        try:
            return Resource._resources[resource_or_resource_name]
        except KeyError:
            raise RuntimeError(f'Unknown resource: "{resource_or_resource_name}"')

    def __repr__(self):
        return '<{} mapping {} to {}>'.format(self.__class__.__name__,
                                              self.model.__name__,
                                              self.model_message.__name__)


class ResourceConverterBase(object):
    _reference: Union[Resource, str]

    def __init__(self, resource_or_resource_name: Union[Resource, str]) -> None:
        self._reference = resource_or_resource_name

    @cached_property
    def resource(self) -> Resource:
        return Resource.resolve(self._reference)


class ResourceEntityConverter(ResourceConverterBase, Converter):
    wire: Type[Message]

    def __init__(self,
                 model_message: Type[Message],
                 resource_or_resource_name: Union[Resource, str]) -> None:
        super().__init__(resource_or_resource_name)
        self.wire = model_message

    @cached_property
    def python(self):
        return self.resource.model

    def convert(self, message: Message) -> Any:
        # TODO fallback where the Python is the same as wire.
        raise NotImplementedError()

    def format(self, entity: Any) -> Message:
        # TODO fallback where the Python is the same as wire.
        return from_object(self.wire, entity)


class ResourceEntityResolver(Resolver):
    def __init__(self, resource: Resource):
        self.resource = resource

    @property
    def python(self):
        return self.resource.model

    async def resolve(self, service: Service, request: Message) -> Any:
        return self.resource.get_from_message(request)


class ResourceEntityIDConverter(ResourceConverterBase, Converter):
    def __init__(self,
                 model_id_type: Union[int, str],
                 resource_or_resource_name: Union[Resource, str]) -> None:
        super().__init__(resource_or_resource_name)
        self.wire = model_id_type

    @cached_property
    def python(self):
        return self.resource.model

    def convert(self, id_: Any) -> Any:
        return self.resource.get(id_)

    def format(self, entity: Any) -> Any:
        return self.resource.get_entity_id(entity)
