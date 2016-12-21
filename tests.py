from flask_sqlalchemy import SQLAlchemy
from venom import Message
from venom.exceptions import NotFound
from venom.fields import String, Bool, Integer, Int32
from venom.rpc import Service, http

from flask_venom.test_utils import TestCase
from flask_venom import Venom
from venom.rpc.test_utils import AioTestCaseMeta
from venom.util import AttributeDict

from venom_entities import ModelService, entity_http, ModelServiceManager


class PetEntity(Message):
    id = Int32()
    name = String()


class ModelServiceTestCase(TestCase, metaclass=AioTestCaseMeta):
    def setUp(self):
        super().setUp()
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.app.config['SQLALCHEMY_ENGINE'] = 'sqlite://'
        self.sa = SQLAlchemy(self.app)
        self.venom = Venom(self.app)

    async def test_e2e_create_entity(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        class PetStore(ModelService):
            class Meta:
                model = Pet
                model_message = PetEntity

            @http.POST('', http_status=201)
            def create_pet(self, request: PetEntity) -> Pet:
                return self.__manager__.create_entity(request)

        self.assertEqual(PetStore.create_pet.response, PetEntity)
        self.venom.add(PetStore)

        with self.app.app_context():
            pet = PetStore().create_pet(PetEntity(name='snek'))
            self.assertIsInstance(pet, Pet)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

        with self.app.app_context():
            pet = await PetStore.create_pet.invoke(PetStore(), PetEntity(name='snek'))
            self.assertIsInstance(pet, PetEntity)
            self.assertEqual(pet, PetEntity(2, 'snek'))

    async def test_e2e_entity_method(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        class PetStore(ModelService):
            class Meta:
                model = Pet
                model_message = PetEntity

            class GetPetRequest(Message):
                pet_id = Int32()

            @entity_http.GET('/{pet_id}', request=GetPetRequest)
            def get_pet(self, pet: Pet) -> Pet:
                return pet

        self.assertEqual(PetStore.get_pet.request, PetStore.GetPetRequest)
        self.assertEqual(PetStore.get_pet.response, PetEntity)

        self.venom.add(PetStore)

        with self.app.app_context():
            PetStore().__manager__.create_entity(PetEntity(name='snek'))

        with self.app.app_context():
            pet = Pet.query.filter(Pet.id == 1).one()
            pet = PetStore().get_pet(pet)

            self.assertIsInstance(pet, Pet)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

        with self.app.app_context():
            pet = await PetStore.get_pet.invoke(PetStore(), PetStore.GetPetRequest(pet_id=1))
            self.assertIsInstance(pet, PetEntity)
            self.assertEqual(pet, PetEntity(1, 'snek'))

        with self.app.app_context():
            with self.assertRaises(NotFound):
                await PetStore.get_pet.invoke(PetStore(), PetStore.GetPetRequest(pet_id=2))


class ModelServiceManagerTestCase(TestCase):

    def setUp(self):
        super().setUp()
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.app.config['SQLALCHEMY_ENGINE'] = 'sqlite://'
        self.sa = SQLAlchemy(self.app)

    def test_create_entity(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        manager = ModelServiceManager(Service, AttributeDict(model=Pet,
                                                             model_message=PetEntity), AttributeDict())

        pet = manager.create_entity(PetEntity())
        self.assertIsInstance(pet, Pet)
        self.assertEqual(pet.id, 1)
        self.assertEqual(pet.name, None)

    # TODO remaining methods