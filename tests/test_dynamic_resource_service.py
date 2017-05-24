from flask_sqlalchemy import SQLAlchemy
from flask_venom import Venom
from flask_venom.test_utils import TestCase
from venom import Message
from venom.common import FieldMask
from venom.common.types import JSONObject, JSONValue
from venom.exceptions import NotFound
from venom.fields import Integer, String, Field, RepeatField
from venom.message import fields, Empty
from venom.rpc.test_utils import AioTestCaseMeta

from venom_resource import SQLAlchemyResource
from venom_resource.service import DynamicResourceService


class DynamicResourceServiceTestCase(TestCase, metaclass=AioTestCaseMeta):
    def setUp(self):
        super().setUp()
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.app.config['SQLALCHEMY_ENGINE'] = 'sqlite://'
        self.sa = SQLAlchemy(self.app)
        self.venom = Venom(self.app)

    def _setup_pet_service_case(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        class PetMessage(Message):
            id = Integer()
            name = String()

        self.sa.create_all()

        class PetService(DynamicResourceService):
            __resource__ = SQLAlchemyResource(Pet, PetMessage)

        self.venom.add(PetService)
        return Pet, PetMessage, PetService

    async def test_e2e_get_entity(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.get.http_path, '/pet/{pet_id}')
        self.assertEqual(PetService.get.response, PetMessage)
        self.assertEqual(fields(PetService.get.request), (Integer(name='id'),))

        with self.app.app_context():
            PetService.__resource__.create(PetMessage(name='snek'))
            pet = await self.venom.get_instance(PetService).get(PetService.get.request(1))
            self.assertIsInstance(pet, PetMessage)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

            with self.assertRaises(NotFound):
                await self.venom.get_instance(PetService).get(PetService.get.request(2))

    async def test_e2e_create_entity(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.create.http_path, '/pet')
        self.assertEqual(PetService.create.name, 'create_pet')

        self.assertEqual(PetService.create.request, PetMessage)
        self.assertEqual(PetService.create.response, PetMessage)

        with self.app.app_context():
            pet = await self.venom.get_instance(PetService).create(PetMessage(name='snek'))
            self.assertIsInstance(pet, PetMessage)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

    async def test_e2e_update_entity(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.update.http_path, '/pet/{pet_id}')
        self.assertEqual(PetService.update.name, 'update_pet')

        self.assertEqual(PetService.update.response, PetMessage)
        self.assertEqual(fields(PetService.update.request), (
            Field(FieldMask, name='update_mask'),
            Integer(name='pet_id'),
            Field(PetMessage, name='pet')
        ))

        with self.app.app_context():
            PetService.__resource__.create(PetMessage(name='snek'))
            pet = await self.venom.get_instance(PetService).update(
                PetService.update.request(pet_id=1,
                                          pet=PetMessage(name='noodle'),
                                          update_mask=FieldMask(['name'])))
            self.assertIsInstance(pet, PetMessage)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'noodle')

    async def test_e2e_delete_entity(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.delete.http_path, '/pet/{pet_id}')
        self.assertEqual(PetService.delete.name, 'delete_pet')

        self.assertEqual(fields(PetService.delete.request), (Integer(name='id'),))
        self.assertEqual(PetService.delete.response, Empty)

        with self.app.app_context():
            pet = await self.venom.get_instance(PetService).create(PetMessage(name='snek'))
            response = await self.venom.get_instance(PetService).delete(PetService.delete.request(pet.id))
            self.assertIsInstance(response, Empty)

            with self.assertRaises(NotFound):
                await self.venom.get_instance(PetService).get(PetService.get.request(pet.id))

    async def test_e2e_list_entities(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.list.http_path, '/pet')
        self.assertEqual(PetService.list.name, 'list_pets')

        self.assertEqual(fields(PetService.list.request), (
            Field(JSONObject, name='filters', schema=PetService.__resource__.filter_schema),
            RepeatField(JSONValue, schema=PetService.__resource__.order_schema, name='order'),
            String(name='page_token'),
            Integer(name='page_size')
        ))

        self.assertEqual(fields(PetService.list.response), (
            String(name='next_page_token'),
            Integer(name='total'),
            RepeatField(PetMessage, name='items')
        ))

        with self.app.app_context():
            pets = await self.venom.get_instance(PetService).list(PetService.list.request(filters={}))
            self.assertEquals(pets, PetService.list.response('', 0, []))  # FIXME [] should equal empty

            pet_1 = await self.venom.get_instance(PetService).create(PetMessage(name='snek'))
            pet_2 = await self.venom.get_instance(PetService).create(PetMessage(name='noodle'))

            pets = await self.venom.get_instance(PetService).list(PetService.list.request())
            self.assertEquals(pets, PetService.list.response('', 2, [pet_1, pet_2]))
