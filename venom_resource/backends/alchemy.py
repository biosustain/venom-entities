from flask import current_app
from flask_sqlalchemy import get_state
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.exc import NoResultFound
from typing import Type, Set, Iterable, Any, Mapping, Tuple, List
from venom.common import FieldMask
from venom.exceptions import NotFound, Conflict
from venom.message import fields, items
from venom.rpc import Service

from venom_resource import Relationship
from venom_resource.resource import Resource, _Mo, _Mo_id, _M


class SQLAlchemyResource(Resource[_Mo, _Mo_id, _M]):
    """

    .. attribute:: request_id_field_name

        The name of the id field used in messages such as GetEntityRequest. Defaults to
        "{model_name}_{model_id_attribute}".

    """
    name: str = None

    model: Type[_Mo]
    model_name: str = None

    model_message: Type[_M]

    model_id_column: 'sqlalchemy.Column' = None
    model_id_attribute: str = None
    model_id_type: Type[_Mo_id] = int  # TODO

    read_only_field_names: Set[str]

    request_id_field_name: str

    default_page_size: int = 50
    maximum_page_size: int = 100

    def __init__(self, model: Type[_Mo],
                 model_message: Type[_M],
                 *,
                 model_name: str = None,
                 name: str = None,
                 relationships: Iterable[Relationship] = ()) -> None:
        super().__init__(model, model_message, name=name, model_name=model_name)
        self._inspect_model(model)

        self._relationships = {
            r.field_name: r for r in relationships
        }

        for field in fields(model_message):
            if field.options.get('relationship'):
                reference, name = field.options.relationship
                self._relationships[field.name] = Relationship(reference, name, field.name)

        self.request_id_field_name = f'{self.model_name}_{self.model_id_attribute}'
        self.request_path = f'./{{{self.request_id_field_name}}}'

    def _inspect_model(self, model: Type[_Mo]) -> None:
        if not self.model_name:
            self.model_name = model_name = model.__tablename__.lower()
            self.model_plural_name = f'{model_name}s'

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
        super().__set_name__(owner, name)

        from ..service import ResourceService
        if issubclass(owner, Service):
            self.default_page_size = owner.__meta__.get('default_page_size') or self.default_page_size
            self.maximum_page_size = owner.__meta__.get('maximum_page_size') or self.maximum_page_size
            owner.__meta__.converters += self.entity_converter,

        if issubclass(owner, ResourceService):
            owner.__resources__.add(self)

    def get_from_message(self, message: _M) -> _Mo:
        return self.get(message[self.request_id_field_name])

    def get(self, id_: _Mo_id, *filters: Any) -> _Mo:
        try:
            query = self.model.query

            if filters:
                query = query.filter(*filters)

            return query.filter(self.model_id_column == id_).one()
        except NoResultFound as e:
            raise NotFound()  # TODO custom messages

    # TODO return a proxy object for paginate(), create() etc.
    # def __get__(self, instance, owner):

    def get_entity_id(self, entity: _Mo) -> Any:
        if hasattr(entity, '__getitem__'):
            try:
                return entity[self.model_id_attribute]
            except (IndexError, TypeError, KeyError):
                return getattr(entity, self.model_id_attribute)

    def paginate(self,
                 page_token: str = '',
                 page_size: int = 0,
                 *filters: Any) -> Tuple[List[_M], str]:

        if self.default_sort_reverse:
            order_clause = self.default_sort_column.desc()
        else:
            order_clause = self.default_sort_column.asc()

        query = self.model.query.order_by(order_clause)

        if filters:
            query = query.filter(*filters)

        if page_size:
            query = query.limit(page_size or 50)

        return query.all(), ''

    def create(self, properties: _M) -> _Mo:
        entity = self.model()
        session = self._session()

        try:
            for name, value in items(properties):
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

    def update(self, entity: _Mo, changes: Mapping[str, Any], mask: FieldMask) -> _Mo:
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

    def delete(self, entity: _Mo) -> None:
        session = self._session()
        session.delete(entity)
        session.commit()

    def format(self, entity: _Mo) -> _M:
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

    def format_id(self, entity: _Mo) -> _Mo_id:
        return getattr(entity, self.model_id_attribute)