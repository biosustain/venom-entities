from typing import Generic, Type, Set, Dict, Any, Mapping, Union, TypeVar, NamedTuple, Iterable

from flask import current_app
from flask_sqlalchemy import get_state
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.exc import NoResultFound
from venom.common import FieldMask, Message, Converter
from venom.exceptions import NotFound, Conflict
from venom.message import fields, from_object
from venom.rpc import Service
from venom.rpc.method import MethodDecorator, HTTPMethodDecorator
from venom.rpc.resolver import Resolver
from venom.util import cached_property

from .messages import ListEntitiesResult
from .methods import EntityMethod

E = TypeVar('E')

E_id = TypeVar('E_id')

M = TypeVar('M', bound=Message)


class _Relationship(NamedTuple):
    reference: Union['EntityResource', str]
    name: str
    field_name: str


class Relationship(_Relationship):
    @property
    def resource(self) -> 'EntityResource':
        return EntityResource.resolve(self.reference)


class EntityResource(Generic[E, E_id, M]):
    """

    .. attribute:: request_id_field_name

        The name of the id field used in messages such as GetEntityRequest. Defaults to
        "{model_name}_{model_id_attribute}".

    """
    name: str = None

    model: Type[E]
    model_name: str = None

    model_message: Type[M]

    model_id_column: 'sqlalchemy.Column' = None
    model_id_attribute: str = None
    model_id_type: E_id = int  # TODO

    read_only_field_names: Set[str]

    request_id_field_name: str

    default_page_size: int = 50
    maximum_page_size: int = 100

    __resources: Dict[str, 'EntityResource'] = {}

    def __init__(self, model: Type[E],
                 model_message: Type[M],
                 *,
                 model_name: str = None,
                 name: str = None,
                 relationships: Iterable[Relationship] = ()) -> None:
        self._inspect_model(model)

        self.model = model
        self.model_message = model_message

        self._relationships = {
            r.field_name: r for r in relationships
        }

        for field in fields(model_message):
            if field.options.get('relationship'):
                reference, name = field.options.relationship
                self._relationships[field.name] = Relationship(reference, name, field.name)

        if model_name:
            self.model_name = model_name

        if name:
            self.name = name
            self.__resources[name] = self

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

    def __set_name__(self, owner, name):
        if not self.name:
            self.name = name
            self.__resources[name] = self

    def get_from_message(self, message: M) -> E:
        return self.get(message[self.request_id_field_name])

    def get(self, entity_id: Any, *filters: Any) -> E:
        try:
            query = self.model.query

            if filters:
                query = query.filter(*filters)

            return query.filter(self.model_id_column == entity_id).one()
        except NoResultFound as e:
            raise NotFound()  # TODO custom messages

    # TODO return a proxy object for paginate(), create() etc.
    # def __get__(self, instance, owner):

    def get_entity_id(self, entity: E) -> Any:
        if hasattr(entity, '__getitem__'):
            try:
                return entity[self.model_id_attribute]
            except (IndexError, TypeError, KeyError):
                return getattr(entity, self.model_id_attribute)

    def prepare(self, manager: 'ResourceServiceManager') -> 'EntityResource':
        """
        An EntityResource always takes its configuration from the service where it is defined.
        """
        self.default_page_size = manager.meta.get('default_page_size') or self.default_page_size
        self.maximum_page_size = manager.meta.get('maximum_page_size') or self.maximum_page_size
        return self

    def paginate(self,
                 page_token: str = '',
                 page_size: int = 0,
                 *filters: Any) -> ListEntitiesResult:

        if self.default_sort_reverse:
            order_clause = self.default_sort_column.desc()
        else:
            order_clause = self.default_sort_column.asc()

        query = self.model.query.order_by(order_clause)

        if filters:
            query = query.filter(*filters)

        if page_size:
            query = query.limit(page_size or 50)

        return ListEntitiesResult(query.all())

    def create(self, properties: Mapping[str, Any]) -> E:
        entity = self.model()
        session = self._session()

        try:
            for name, value in properties.items():
                if name not in self.read_only_field_names:
                    if name in self._relationships:
                        relationship = self._relationships[name]
                        resource = self.resolve(relationship.resource)
                        setattr(entity, relationship.name, resource.get(value))
                    else:
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
                if not mask.match_path(field.name):
                    continue

                if field.name not in self.read_only_field_names:
                    if field.name in self._relationships:
                        # TODO ToMany relationships
                        if changes.get(field.name):
                            relationship = self._relationships[field.name]
                            resource = self.resolve(relationship.resource)
                            setattr(entity, relationship.name, resource.get(changes.get(field.name)))
                        else:
                            setattr(entity, field.name, None)
                    else:
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

    def format(self, entity: E) -> M:
        message = self.model_message()
        for field in fields(self.model_message):
            if field.name in self._relationships:
                relationship = self._relationships[field.name]
                resource = self.resolve(relationship.resource)
                relationship_entity = getattr(entity, relationship.name)
                if relationship_entity is not None:
                    message[field.name] = resource.format_id(relationship_entity)
            else:
                message[field.name] = getattr(entity, field.name)
        return message

    def format_id(self, entity: E) -> E_id:
        return getattr(entity, self.model_id_attribute)

    @cached_property
    def entity_converter(self) -> 'ResourceEntityConverter':
        return ResourceEntityConverter(self.model_message, self)

    @cached_property
    def entity_resolver(self) -> 'ResourceEntityResolver':
        return ResourceEntityResolver(self)

    @cached_property
    def rpc(self):
        return MethodDecorator(EntityMethod, resource=self)

    @cached_property
    def http(self):
        return HTTPMethodDecorator(EntityMethod, resource=self)

    @classmethod
    def resolve(cls, resource_or_resource_name: Union['EntityResource', str]):
        if isinstance(resource_or_resource_name, EntityResource):
            return resource_or_resource_name
        try:
            return EntityResource.__resources[resource_or_resource_name]
        except KeyError:
            raise RuntimeError(f'Unknown resource: "{resource_or_resource_name}"')

    def __repr__(self):
        return '<{} mapping {} to {}>'.format(self.__class__.__name__, self.model.__name__, self.model_message.__name__)


class ResourceConverterBase(object):
    _reference: Union[EntityResource, str]

    def __init__(self, resource_or_resource_name: Union[EntityResource, str]) -> None:
        self._reference = resource_or_resource_name

    @cached_property
    def resource(self) -> EntityResource:
        return EntityResource.resolve(self._reference)


class ResourceEntityConverter(ResourceConverterBase, Converter):
    wire: Type[Message]

    def __init__(self,
                 model_message: Type[Message],
                 resource_or_resource_name: Union[EntityResource, str]) -> None:
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
    def __init__(self, resource: EntityResource):
        self.resource = resource

    @property
    def python(self):
        return self.resource.model

    async def resolve(self, service: Service, request: Message) -> Any:
        return self.resource.get_from_message(request)


class ResourceEntityIDConverter(ResourceConverterBase, Converter):
    def __init__(self,
                 model_id_type: Union[int, str],
                 resource_or_resource_name: Union[EntityResource, str]) -> None:
        super().__init__(resource_or_resource_name)
        self.wire = model_id_type

    @cached_property
    def python(self):
        return self.resource.model

    def convert(self, id_: Any) -> Any:
        return self.resource.get(id_)

    def format(self, entity: Any) -> Any:
        return self.resource.get_entity_id(entity)
