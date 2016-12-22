from collections import namedtuple
from functools import partial
from typing import Any, Tuple, Sequence, Dict, Mapping, NewType, TypeVar, Generic, Union, List
from typing import Type

from flask import current_app
from flask_sqlalchemy import get_state
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.exc import NoResultFound
from venom import Message
from venom.common import FieldMask
from venom.converter import Converter
from venom.exceptions import NotFound, Conflict
from venom.fields import ConverterField
from venom.message import from_object, fields
from venom.rpc.method import ServiceMethod, rpc, Method, http, HTTPVerb, http_method_decorator
from venom.rpc.resolver import Resolver
from venom.rpc.service import ServiceManager, Service
from venom.util import MetaDict, cached_property

E = TypeVar('E')


class ListEntitiesResult(Generic[E]):
    def __init__(self,
                 entities: List[E],
                 next_page_token: str = None):
        self.entities = entities
        self.next_page_token = next_page_token


class ModelServiceManager(ServiceManager):
    model = None  # type: type
    model_name = None  # type: Any
    model_message = None  # type: Type[Message]

    # SQLAlchemy-specific:
    model_id_column = None  # type: sqlalchemy.Column
    model_id_attribute = None  # type: str

    def __new__(cls, service: Type[Service], meta: MetaDict, meta_changes: MetaDict):
        # fall back to ServiceManager if there is no entity and therefore no reason to use ModelServiceManager
        if meta.get('model'):
            obj = object.__new__(cls)
            obj.__init__(service, meta, meta_changes)
            return obj
        return ServiceManager(service, meta, meta_changes)

    def __init__(self, service: Type[Service], meta: MetaDict, meta_changes: MetaDict) -> None:
        super().__init__(service, meta, meta_changes)
        self.model = model = meta.model
        self.model_name = self._meta_get_model_name(meta, meta_changes)
        self.model_message = meta.model_message

        if meta.get('model_id_attribute'):
            self.model_id_column = getattr(model, meta.model_id_attribute)
            self.model_id_attribute = meta.model_id_attribute
        else:
            mapper = class_mapper(model)
            self.model_id_column = model_id_column = mapper.primary_key[0]
            self.model_id_attribute = model_id_column.name

        self.read_only_field_names = {self.model_id_attribute}

        self.default_sort_column = self.model_id_column
        self.default_sort_reverse = False

    @staticmethod
    def _meta_get_model_name(meta: MetaDict, meta_changes: MetaDict) -> str:
        model_name = meta.get('model_name')
        if not model_name:
            model = meta['model']
            return model.__tablename__.lower()
        return str(model_name)

    def register_method(self, method: Method, name: str) -> Method:
        if isinstance(method, ServiceMethod):
            return method.register(self.service, method.name or name, converters=[self.entity_converter])
        return method.register(self.service, method.name or name)

    @cached_property
    def entity_converter(self) -> 'ModelEntityConverter':
        return ModelEntityConverter(self.model, self.model_message)

    @cached_property
    def entity_id_field_name(self) -> str:
        return self.model_name + '_' + self.model_id_attribute

    @staticmethod
    def session():
        # XXX reference to current_app would have to be in context if this wasn't synchronous. Use RequestContext.
        return get_state(current_app).db.session

    def get_entity(self, entity_id: Any) -> Any:
        try:
            return self.model.query.filter(self.model_id_column == entity_id).one()
        except NoResultFound as e:
            raise NotFound()  # TODO custom messages

    # def _encode_next_page_cursor(self, previous_value: str, offset: int = 0, reverse: bool = False) -> str:
    #     pass
    #
    # def _decode_cursor(self, cursor: str) -> Tuple[str, int, bool]:
    #     pass

    def list_entities(self,
                      page_token: str = None,
                      page_size: int = None) -> ListEntitiesResult:

        if self.default_sort_reverse:
            order_clause = self.default_sort_column.desc()
        else:
            order_clause = self.default_sort_column.asc()

        query = self.model.query.order_by(order_clause)

        if page_size:
            query = query.limit(page_size or 50)

        return ListEntitiesResult(query.all())

    def create_entity(self, properties: Mapping[str, Any]) -> Any:
        entity = self.model()
        session = self.session()

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

    def update_entity(self, entity: Any, changes: Mapping[str, Any], mask: FieldMask) -> Any:
        session = self.session()

        try:
            for field in fields(self.model_message):
                if field.name not in self.read_only_field_names and mask.match_path(field.name):
                    setattr(entity, field.name, changes.get(field.name))
            session.commit()
        except IntegrityError as e:
            session.rollback()
            raise Conflict()

        return entity

    def delete_entity(self, entity: Any) -> None:
        session = self.session()
        session.delete(entity)
        session.commit()


class ModelEntityConverter(Converter):
    wire = None  # type: Type[Message]
    python = None

    def __init__(self, model: type, model_message: Type[Message]) -> None:
        self.wire = model_message
        self.python = model

    def convert(self, message: Message) -> Any:
        raise RuntimeError()

    def format(self, entity: Any) -> Message:
        return from_object(self.wire, entity)


def inline(model: type, model_message: Type[Message]) -> ConverterField:
    return ConverterField(model_message, converter=ModelEntityConverter(model, model_message))


class ModelEntityResolver(Resolver):
    def __init__(self, manager: ModelServiceManager):
        self.manager = manager

    @property
    def python(self):
        return self.manager.model

    async def resolve(self, service: Service, request: Message) -> Any:
        # FIXME request[field] should always have a value (fallback to default)
        entity_id = request.get(self.manager.entity_id_field_name)
        return self.manager.get_entity(entity_id)


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

    def register(self,
                 service: Type['venom.rpc.service.Service'],
                 name: str,
                 *args: Tuple[Resolver, ...],
                 converters: Sequence[Converter] = ()) -> 'EntityMethod':
        if not isinstance(service.__manager__, ModelServiceManager):
            raise TypeError("An EntityMethod can only be used with a ModelService")
        return super().register(service, name, ModelEntityResolver(service.__manager__), *args, converters=converters)


entity_rpc = partial(rpc, method_cls=EntityMethod)

entity_http = partial(http, method_cls=EntityMethod)

for _verb in HTTPVerb:
    setattr(entity_http, _verb.name, http_method_decorator(_verb, method_cls=EntityMethod))


class ModelService(Service):
    __manager__ = None  # type: ModelServiceManager

    class Meta:
        model = None
        model_message = None
        manager = ModelServiceManager

        # SQLAlchemy specific?
        model_id_attribute = 'id'
