from typing import NamedTuple, Union, Tuple

from flask_sqlalchemy import SQLAlchemy
from flask_venom import Venom
from flask_venom.test_utils import TestCase
from venom import Message
from venom.common import FieldMask
from venom.exceptions import NotFound
from venom.fields import String, Int32
from venom.rpc import http
from venom.rpc.method import ServiceMethod
from venom.rpc.test_utils import AioTestCaseMeta

from venom_resource import SQLAlchemyResource, Relationship, ResourceService
from venom_resource.methods import EntityMethodDescriptor


class PetEntity(Message):
    id = Int32()
    name = String()


class ResourceServiceTestCase(TestCase, metaclass=AioTestCaseMeta):
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

        class PetStore(ResourceService):
            pets = SQLAlchemyResource(Pet, PetEntity)

            class Meta:
                default_page_size = 42

            @http.POST('', http_status=201)
            def create_pet(self, request: PetEntity) -> Pet:
                return self.pets.create(request)

        self.assertEqual(PetStore.pets.default_page_size, 42)
        self.assertEqual(PetStore.create_pet.response, PetEntity)
        self.venom.add(PetStore)

        with self.app.app_context():
            pet = await PetStore().create_pet(PetEntity(name='snek'))
            self.assertIsInstance(pet, PetEntity)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

    async def test_e2e_entity_method(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        class PetStore(ResourceService):
            pets = SQLAlchemyResource(Pet, PetEntity)

            class GetPetRequest(Message):
                pet_id = Int32()

            @pets.http.GET('/{pet_id}', request=GetPetRequest)
            def get_pet(self, pet: Pet) -> Pet:
                return pet

        self.assertIn(PetStore.pets.entity_converter, PetStore.__meta__.converters)
        self.assertIsInstance(PetStore.__method_descriptors__['get_pet'], EntityMethodDescriptor)
        self.assertIsInstance(PetStore.get_pet, ServiceMethod)

        self.assertIsInstance(PetStore.pets, SQLAlchemyResource)
        self.assertEqual(PetStore.get_pet.request, PetStore.GetPetRequest)
        self.assertEqual(PetStore.get_pet.response, PetEntity)

        self.venom.add(PetStore)

        with self.app.app_context():
            PetStore().pets.create(PetEntity(name='snek'))

        with self.app.app_context():
            pet = await PetStore().get_pet(PetStore.GetPetRequest(pet_id=1))
            self.assertIsInstance(pet, PetEntity)
            self.assertEqual(pet, PetEntity(1, 'snek'))

        with self.app.app_context():
            with self.assertRaises(NotFound):
                await PetStore().get_pet(PetStore.GetPetRequest(pet_id=2))


class ResourceServiceManagerTestCase(TestCase):
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

        pets = SQLAlchemyResource(Pet, PetEntity)

        pet = pets.create(PetEntity())
        self.assertIsInstance(pet, Pet)
        self.assertEqual(pet.id, 1)
        self.assertEqual(pet.name, None)

    def test_update_entity(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        resource = SQLAlchemyResource(Pet, PetEntity)

        with self.app.app_context():
            pet = Pet(name='snek')
            self.sa.session.add(pet)
            self.sa.session.commit()

        with self.app.app_context():
            pet = Pet.query.filter(Pet.id == 1).one()
            pet = resource.update(pet, PetEntity(name='noodle'), FieldMask())
            self.assertIsInstance(pet, Pet)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'snek')

        with self.app.app_context():
            pet = Pet.query.filter(Pet.id == 1).one()
            pet = resource.update(pet, PetEntity(id=5, name='noodle'), FieldMask(['id', 'name', 'foo']))
            self.assertIsInstance(pet, Pet)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, 'noodle')

        with self.app.app_context():
            pet = Pet.query.filter(Pet.id == 1).one()
            pet = resource.update(pet, PetEntity(), FieldMask(['name']))
            self.assertIsInstance(pet, Pet)
            self.assertEqual(pet.id, 1)
            self.assertEqual(pet.name, '')

    def test_delete_entity(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        with self.app.app_context():
            pet = Pet()
            self.sa.session.add(pet)
            self.sa.session.commit()

        resource = SQLAlchemyResource(Pet, PetEntity)

        with self.app.app_context():
            pet = Pet.query.filter(Pet.id == 1).one()
            resource.delete(pet)

        with self.app.app_context():
            self.assertEqual(Pet.query.all(), [])

    def test_list_entities(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        with self.app.app_context():
            self.sa.session.add(Pet(name='snek'))
            self.sa.session.add(Pet(name='noodle'))
            self.sa.session.commit()

        resource = SQLAlchemyResource(Pet, PetEntity)

        with self.app.app_context():
            items, next_page_token = resource.paginate()
            self.assertEqual([pet.name for pet in items], ['snek', 'noodle'])

    def _create_person_pet_scenario(self) -> Tuple[type, type]:
        class Person(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)

        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            name = self.sa.Column(self.sa.String(), nullable=True)
            owner_id = self.sa.Column(self.sa.Integer(), self.sa.ForeignKey(Person.id))
            owner = self.sa.relationship(Person)

        self.sa.create_all()
        return Person, Pet

    def test_relationship_from_field_option(self):
        Person, Pet = self._create_person_pet_scenario()

        class PersonEntity(Message):
            id = Int32()
            name = String()

        class PetEntity(Message):
            id = Int32()
            name = String()
            owner_id = Int32(relationship=('person', 'owner'))

        people = SQLAlchemyResource(Person, PersonEntity, name='person')
        pets = SQLAlchemyResource(Pet, PetEntity)

        self.assertEqual(pets._relationships, {
            'owner_id': Relationship('person', 'owner', 'owner_id')
        })
        self.assertEqual(pets._relationships['owner_id'].resource, people)

    def test_relationship_to_one(self):
        Person, Pet = self._create_person_pet_scenario()

        class PersonEntity(Message):
            id = Int32()
            name = String()

        class PetEntity(Message):
            id = Int32()
            name = String()
            owner_id = Int32()

        self.sa.create_all()

        people = SQLAlchemyResource(Person, PersonEntity)
        pets = SQLAlchemyResource(Pet, PetEntity, relationships={
            Relationship(people, 'owner', 'owner_id')
        })

        with self.app.app_context():
            snek = pets.create(PetEntity(name='snek'))

            self.assertEqual(pets.format(snek), PetEntity(1, 'snek'))

            with self.assertRaises(NotFound):
                pets.update(snek, PetEntity(owner_id=1), FieldMask(['owner_id']))

            foo = people.create(PersonEntity(name='foo'))
            self.assertEqual(people.format(foo), PersonEntity(1, 'foo'))

            snek = pets.update(snek, PetEntity(owner_id=1), FieldMask(['owner_id']))
            self.assertEqual(pets.format(snek), PetEntity(1, 'snek', owner_id=1))

            snek = pets.update(snek, PetEntity(), FieldMask(['owner_id']))
            self.assertEqual(pets.format(snek), PetEntity(1, 'snek'))

            fluff = pets.create(PetEntity(name='fluff', owner_id=1))
            self.assertEqual(pets.format(fluff), PetEntity(2, 'fluff', owner_id=1))
