from typing import ClassVar

from venom.rpc import Service

from .resource import EntityResource


class ResourceService(Service):
    __resources__: ClassVar[EntityResource] = set()

    class Meta:
        default_page_size: int = None
        maximum_page_size: int = 100
