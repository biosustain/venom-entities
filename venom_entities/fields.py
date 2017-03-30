from typing import Type, Union, Any

from venom import Message
from venom.fields import ConverterField, String

from venom_entities import EntityResource, ResourceEntityConverter


class EntityField(ConverterField):
    def __init__(self,
                 model_message: Type[Message],
                 resource_or_resource_name: Union[EntityResource, str]) -> None:
        super().__init__(Any, converter=ResourceEntityConverter(model_message, resource_or_resource_name))


class EntityReference(Message):
    uri = String()


class ToOne(ConverterField):
    pass

