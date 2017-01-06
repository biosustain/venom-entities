from typing import Any, Tuple, Sequence, MutableMapping, TypeVar, Generic, List, Set, Mapping
from typing import Type

import sqlalchemy
from flask import current_app
from flask_sqlalchemy import get_state
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.exc import NoResultFound
from venom import Message
from venom.common import FieldMask, String
from venom.converter import Converter
from venom.exceptions import NotFound, Conflict
from venom.fields import ConverterField, Integer
from venom.message import from_object, fields
from venom.rpc.method import ServiceMethod, Method, HTTPMethodDecorator, MethodDecorator
from venom.rpc.resolver import Resolver
from venom.rpc.service import ServiceManager, Service
from venom.util import MetaDict, cached_property

E = TypeVar('E')


class ListEntitiesRequest(Message):
    page_token = String()
    page_size = Integer()


class ListEntitiesResult(Generic[E]):
    def __init__(self,
                 entities: List[E],
                 next_page_token: str = None):
        self.entities = entities
        self.next_page_token = next_page_token


M = TypeVar('M', bound=Message)


class EntityResource(Generic[E, M]):
    """

    .. attribute:: request_id_field_name

        The name of the id field used in messages such as GetEntityRequest. Defaults to
        "{model_name}_{model_id_attribute}".

    """
    model: Type[E]
    model_name: str = None

    model_message: Type[M]

    model_id_column: 'sqlalchemy.Column' = None
    model_id_attribute: str = None

    read_only_field_names: Set[str]

    request_id_field_name: str

    default_page_size: int = 50
    maximum_page_size: int = 100

    def __init__(self, model: Type[E],
                 model_message: Type[M],
                 *,
                 model_name: str = None) -> None:
        self._inspect_model(model)

        self.model = model
        self.model_message = model_message

        if model_name:
            self.model_name = model_name

        self.request_id_field_name = self.model_name + '_' + self.model_id_attribute

    def _inspect_model(self, model: Type[E]) -> None:
        self.model_name = model.__tablename__.lower()

        mapper = class_mapper(model)
        self.model_id_column = model_id_column = mapper.primary_key[0]
        self.model_id_attribute = model_id_column.name

        self.read_only_field_names = {self.model_id_attribute}
        self.default_sort_column = self.model_id_column
        self.default_sort_reverse = False

    @staticmethod
    def _session():
        # XXX reference to current_app would have to be in context if this wasn't synchronous. Use RequestContext.
        return get_state(current_app).db.session

    def get_from_message(self, message: M) -> E:
        return self.get(message[self.request_id_field_name])

    def get(self, entity_id: Any) -> E:
        try:
            return self.model.query.filter(self.model_id_column == entity_id).one()
        except NoResultFound as e:
            raise NotFound()  # TODO custom messages

    # TODO return a proxy object for paginate(), create() etc.
    # def __get__(self, instance, owner):

    def prepare(self, manager: 'ModelServiceManager') -> 'EntityResource':
        """
        An EntityResource always takes its configuration from the service where it is defined.

        :param service:
        :param meta:
        :return:
        """
        self.default_page_size = manager.meta.get('default_page_size') or self.default_page_size
        self.maximum_page_size = manager.meta.get('maximum_page_size') or self.maximum_page_size
        return self

    def paginate(self,
                 page_token: str = '',
                 page_size: int = 0) -> ListEntitiesResult:

        if self.default_sort_reverse:
            order_clause = self.default_sort_column.desc()
        else:
            order_clause = self.default_sort_column.asc()

        query = self.model.query.order_by(order_clause)

        if page_size:
            query = query.limit(page_size or 50)

        return ListEntitiesResult(query.all())

    def create(self, properties: Mapping[str, Any]) -> E:
        entity = self.model()
        session = self._session()

        try:
            for name, value in properties.items():
                if name not in self.read_only_field_names:
                    setattr(entity, name, value)

            session.add(entity)
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise Conflict()

        return entity

    def update(self, entity: E, changes: Mapping[str, Any], mask: FieldMask) -> E:
        session = self._session()

        try:
            for field in fields(self.model_message):
                if field.name not in self.read_only_field_names and mask.match_path(field.name):
                    setattr(entity, field.name, changes.get(field.name))
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise Conflict()

        return entity

    def delete(self, entity: E) -> None:
        session = self._session()
        session.delete(entity)
        session.commit()

    @cached_property
    def entity_converter(self) -> 'ModelEntityConverter':
        return ModelEntityConverter(self.model, self.model_message)

    @cached_property
    def entity_resolver(self) -> 'ModelEntityResolver':
        return ModelEntityResolver(self)

    @cached_property
    def Inline(self) -> ConverterField:
        return ConverterField(self.model_message, converter=self.entity_converter)

    @cached_property
    def rpc(self):
        return MethodDecorator(EntityMethod, resource=self)

    @cached_property
    def http(self):
        return HTTPMethodDecorator(EntityMethod, resource=self)

    def __repr__(self):
        return '<{} mapping {} to {}>'.format(self.__class__.__name__, self.model.__name__, self.model_message.__name__)


class ModelServiceManager(ServiceManager):
    resources: Set[EntityResource]

    def __init__(self, meta: MetaDict, meta_changes: MetaDict) -> None:
        super().__init__(meta, meta_changes)
        self.resources = set()

    def prepare_members(self, members: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
        for name, member in members.items():
            # FIXME only look at members
            if isinstance(member, EntityResource):
                members[name] = resource = member.prepare(self)
                self.resources.add(resource)

        return super().prepare_members(members)

    def prepare_method(self, method: Method, name: str) -> Method:
        if isinstance(method, ServiceMethod):
            return method.prepare(self,
                                  method.name or name,
                                  converters=[resource.entity_converter for resource in self.resources])
        return method.prepare(self, method.name or name)


class ModelEntityConverter(Converter):
    wire: Type[Message]
    python: Any

    def __init__(self, model: type, model_message: Type[Message]) -> None:
        self.wire = model_message
        self.python = model

    def convert(self, message: Message) -> Any:
        raise RuntimeError()

    def format(self, entity: Any) -> Message:
        return from_object(self.wire, entity)


class ModelEntityResolver(Resolver):
    def __init__(self, resource: EntityResource):
        self.resource = resource

    @property
    def python(self):
        return self.resource.model

    async def resolve(self, service: Service, request: Message) -> Any:
        return self.resource.get_from_message(request)


class EntityMethod(ServiceMethod):
    """

    This method can only be used together with a service that has a :class:`ModelServiceManager`.

    A function wrapped with this method must accept a positional argument that accepts an entity (of the type handled
    by this service). The entity is resolved from an id field that must be located somewhere in the message of the
    request.

    ::

        class UpdateFooEntityRequest(Message):
            foo_id = Int64()
            name = String()


        class FooModelService(ModelService):
            class Meta:
                entity = FooEntity
                entity_name = 'foo'

            @rpc(method_cls=EntityMethod)
            async def update_entity(self, entity: FooEntity, request: UpdateFooEntityRequest) -> FooEntity:
                ...

    """

    def __init__(self, *args, resource: EntityResource, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource = resource

    def prepare(self,
                manager: 'venom.rpc.service.ServiceManager',
                name: str,
                *args: Tuple[Resolver, ...],
                converters: Sequence[Converter] = ()) -> 'EntityMethod':
        return super().prepare(manager,
                               name,
                               self.resource.entity_resolver,
                               *args,
                               converters=converters)


class ModelService(Service):
    class Meta:
        manager = ModelServiceManager
        default_page_size = None
        maximum_page_size = 100
