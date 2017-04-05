from flask_sqlalchemy import SQLAlchemy
from flask_venom.test_utils import TestCase
from flask_venom import Venom
from venom import Message
from venom.common import FieldMask
from venom.exceptions import NotFound
from venom.fields import Integer, String, Field
from venom.message import fields
from venom.rpc.test_utils import AioTestCaseMeta

from venom_entities import SQLAlchemyResource
from venom_entities.service import DynamicResourceService


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

        self.assertEqual(PetService.get.http_path, '/pet/{id}')
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

        self.assertEqual(PetService.create.request, PetMessage)
        self.assertEqual(PetService.create.response, PetMessage)

        with self.app.app_context():
            pet = await self.venom.get_instance(PetService).create(PetMessage(name='snek'))
            self.assertIsInstance(pet, PetMessage)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

    async def test_e2e_update_entity(self):
        Pet, PetMessage, PetService = self._setup_pet_service_case()

        self.assertEqual(PetService.update.http_path, '/pet/{id}')
        self.assertEqual(PetService.update.response, PetMessage)
        self.assertEqual(fields(PetService.update.request), (
            Integer(name='id'),
            Field(PetMessage, name='changes'),
            Field(FieldMask, name='update_mask')
        ))

        with self.app.app_context():
            PetService.__resource__.create(PetMessage(name='snek'))
            pet = await self.venom.get_instance(PetService).update(
                PetService.update.request(1, PetMessage(name='noodle'), FieldMask(['name'])))
            self.assertIsInstance(pet, PetMessage)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'noodle')
