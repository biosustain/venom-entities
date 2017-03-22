from typing import Set, MutableMapping, Any

from venom.rpc import Service
from venom.rpc.method import Method, ServiceMethod
from venom.rpc.service import ServiceManager
from venom.util import MetaDict

from .resource import EntityResource


class ResourceServiceManager(ServiceManager):
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


class ResourceService(Service):
    class Meta:
        manager = ResourceServiceManager
        default_page_size = None
        maximum_page_size = 100
