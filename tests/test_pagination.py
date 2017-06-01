from flask_sqlalchemy import SQLAlchemy
from flask_venom import Venom
from flask_venom.test_utils import TestCase
from sqlalchemy import func
from venom import Message
from venom.common.fields import DateTime
from venom.exceptions import NotFound
from venom.fields import Integer, String
from venom.rpc.test_utils import AioTestCaseMeta

from venom_resource.backends.alchemy.pagination import CursorPagination


class CursorPaginationTestCase(TestCase, metaclass=AioTestCaseMeta):
    def setUp(self):
        super().setUp()
        self.app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        self.app.config['SQLALCHEMY_ENGINE'] = 'sqlite://'
        self.sa = SQLAlchemy(self.app)
        self.venom = Venom(self.app)

    def _setup_pet_service_case(self):
        class Pet(self.sa.Model):
            id = self.sa.Column(self.sa.Integer(), primary_key=True)
            created_at = self.sa.Column(self.sa.DateTime(timezone=True), default=func.now(), nullable=False)
            name = self.sa.Column(self.sa.String(), nullable=True)

        self.sa.create_all()

        return Pet

    async def test_invalid_cursor(self):
        Pet = self._setup_pet_service_case()
        paginator = CursorPagination(Pet, 10, [{'field': 'created_at', 'ascending': False}])

        with self.assertRaises(NotFound):
            paginator.paginate_queryset('123')

    async def test_ordering(self):
        Pet = self._setup_pet_service_case()

        with self.assertRaises(AssertionError):
            CursorPagination(Pet, 4, None)

        with self.assertRaises(AssertionError):
            CursorPagination(Pet, 4, 'created_at')

        with self.assertRaises(AssertionError):
            CursorPagination(Pet, 10, [{'field': 'created_at'}])

        paginator = CursorPagination(Pet, 10, [{'field': 'created_at', 'ascending': False}])
        paginator.paginate_queryset()
        self.assertListEqual(paginator.ordering, [{'field': 'created_at', 'ascending': False}])

    async def test_cursor_pagination(self):
        Pet = self._setup_pet_service_case()

        pet_names = ['D', 'G', 'L', 'I', 'O', 'X', 'Y', 'T', 'U', 'Z', 'M', 'A', 'B', 'S', 'P', 'E', 'C', 'N', 'W', 'V', 'H', 'F', 'R', 'K', 'Q', 'J']

        pets = [Pet(name=name) for name in pet_names]
        self.sa.session.add_all(pets)


        def get_pages(pagination, page_token=None):
            """
            Given a page token return a tuple of:

            (previous page, current page, next page, previous token, next tpken)
            """
            queryset = pagination.paginate_queryset(page_token)
            current = [item.name for item in queryset]

            next_token = pagination.get_next_token()
            previous_token = pagination.get_previous_token()

            if next_token is not None:
                queryset = pagination.paginate_queryset(next_token)
                next = [item.name for item in queryset]
            else:
                next = None

            if previous_token is not None:
                queryset = pagination.paginate_queryset(previous_token)
                previous = [item.name for item in queryset]
            else:
                previous = None

            return (previous, current, next, previous_token, next_token)

        pagination = CursorPagination(Pet, 2, [{'field': 'name', 'ascending': True}])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=None)
        self.assertEqual(previous, None)
        self.assertEqual(previous_token, None)
        self.assertListEqual(current, ['A', 'B'])
        self.assertListEqual(next, ['C', 'D'])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=next_token)
        self.assertListEqual(previous, ['A', 'B'])
        self.assertListEqual(current, ['C', 'D'])
        self.assertListEqual(next, ['E', 'F'])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=next_token)
        self.assertListEqual(previous, ['C', 'D'])
        self.assertListEqual(current, ['E', 'F'])
        self.assertListEqual(next, ['G', 'H'])

        pagination = CursorPagination(Pet, 2, [{'field': 'name', 'ascending': False}])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=None)
        self.assertEqual(previous, None)
        self.assertEqual(previous_token, None)
        self.assertListEqual(current, ['Z', 'Y'])
        self.assertListEqual(next, ['X', 'W'])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=next_token)
        self.assertListEqual(previous, ['Z', 'Y'])
        self.assertListEqual(current, ['X', 'W'])
        self.assertListEqual(next, ['V', 'U'])

        pagination = CursorPagination(Pet, 4, [{'field': 'created_at', 'ascending': True}])
        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=None)
        self.assertListEqual(current, ['D', 'G', 'L', 'I'])
        self.assertListEqual(next, ['O', 'X', 'Y', 'T'])

        pagination = CursorPagination(Pet, 20, [{'field': 'name', 'ascending': True}])
        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=None)
        self.assertEqual(len(current), 20)
        self.assertListEqual(next, ['U', 'V', 'W', 'X', 'Y', 'Z'])

        (previous, current, next, previous_token, next_token) = get_pages(pagination, page_token=next_token)
        self.assertEqual(len(previous), 20)
        self.assertListEqual(current, ['U', 'V', 'W', 'X', 'Y', 'Z'])
        self.assertEqual(next, None)
