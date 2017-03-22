from typing import Tuple, Sequence

from venom.converter import Converter
from venom.rpc.method import ServiceMethod
from venom.rpc.resolver import Resolver


class EntityMethod(ServiceMethod):
    """

    This method can only be used together with a service that has a :class:`ResourceServiceManager`.

    A function wrapped with this method must accept a positional argument that accepts an entity (of the type handled
    by this service). The entity is resolved from an id field that must be located somewhere in the message of the
    request.

    ::

        class UpdateFooEntityRequest(Message):
            foo_id = Int64()
            name = String()


        class FooModelService(ResourceService):
            class Meta:
                entity = FooEntity
                entity_name = 'foo'

            @rpc(method_cls=EntityMethod)
            async def update_entity(self, entity: FooEntity, request: UpdateFooEntityRequest) -> FooEntity:
                ...

    """

    def __init__(self, *args, resource: 'venom_entity.resource.EntityResource', **kwargs):
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
