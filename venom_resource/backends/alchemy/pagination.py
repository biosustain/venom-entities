from base64 import b64decode, b64encode
from collections import namedtuple
from typing import Any, Dict, List, Union
from urllib.parse import parse_qs, urlencode

from flask_sqlalchemy import Model
from sqlalchemy import asc, desc
from venom.exceptions import NotFound


def _positive_int(integer_string, strict=False, cutoff=None):
    """
    Cast a string to a strictly positive integer.
    """
    ret = int(integer_string)
    if ret < 0 or (ret == 0 and strict):
        raise ValueError()
    if cutoff:
        ret = min(ret, cutoff)
    return ret


def _reverse_ordering(ordering_tuple):
    """
    Given an order_by tuple such as `({'field': 'created', 'ascending': False}, {'field': 'uuid', 'ascending': True})`
    reverse the ordering and return a new tuple, eg.
    `({'field': 'created', 'ascending': True}, {'field': 'uuid', 'ascending': False})`.
    
    """
    def invert(x):
        return {
            'field': x['field'],
            'ascending': not x['ascending']
        }

    return tuple([invert(item) for item in ordering_tuple])


def convert_ordering_to_alchmey_clauses(model, ordering):
    clauses = []

    for order in ordering:
        order_field = getattr(model, order['field'])

        if order['ascending']:
            clauses.append(asc(order_field))
        else:
            clauses.append(desc(order_field))

    return clauses


Cursor = namedtuple('Cursor', ['offset', 'reverse', 'position'])
_Ordering_T = Union[List[Dict[str, Any]], Dict[str, Any]]


class CursorPagination(object):
    """
    The cursor pagination implementation is necessarily complex.
    For an overview of the position/offset style we use, see this post:
    http://cramer.io/2011/03/08/building-cursors-for-the-disqus-api
    """
    cursor_query_param = 'cursor'
    page_size = None
    invalid_cursor_message = 'Invalid cursor'

    # The offset in the cursor is used in situations where we have a
    # nearly-unique index. (Eg millisecond precision creation timestamps)
    # We guard against malicious users attempting to cause expensive database
    # queries, by having a hard cap on the maximum possible size of the offset.
    offset_cutoff = 1000

    def __init__(self, model: Model, page_size: int, ordering: _Ordering_T):
        assert isinstance(ordering, (dict, list, tuple)), (
            'Invalid ordering. Expected dict or tuple, but got {type}'.format(
                type=type(ordering).__name__
            )
        )
        if isinstance(ordering, dict):
            ordering = [ordering, ]

        self.model = model
        self.page_size = page_size

        self.ordering = []

        for order in ordering:
            if not hasattr(self.model, order['field']):
                continue

            if order.get('ascending') is None:
                continue

            self.ordering.append(order)

        assert len(self.ordering) > 0, (
            'Invalid ordering. Expected at lease one value with '
            'correct ascending value but got {ordering}'.format(
                ordering=ordering
            )
        )

    def paginate_query(self, query, page_token: str = None):
        self.cursor = self.decode_cursor(page_token)

        if self.cursor is None:
            (offset, reverse, current_position) = (0, False, None)
        else:
            (offset, reverse, current_position) = self.cursor

        # Cursor pagination always enforces an ordering.
        if reverse:
            ordering_clauses = convert_ordering_to_alchmey_clauses(self.model,
                                                                   _reverse_ordering(self.ordering))
        else:
            ordering_clauses = convert_ordering_to_alchmey_clauses(self.model, self.ordering)

        query = query.order_by(*ordering_clauses)

        # If we have a cursor with a fixed position then filter by that.
        if current_position is not None:
            order = self.ordering[0]
            is_reversed = order['ascending'] is False
            order_attr = order['field']

            try:
                column = getattr(self.model, order_attr)
            except AttributeError:
                raise NotFound()

            # Test for: (cursor reversed) XOR (queryset reversed)
            if self.cursor.reverse != is_reversed:
                query = query.filter(column < current_position)
            else:
                query = query.filter(column > current_position)

        # If we have an offset cursor then offset the entire page by that amount.
        # We also always fetch an extra item in order to determine if there is a
        # page following on from this one.
        results = list(query[offset:offset + self.page_size + 1])
        self.page = list(results[:self.page_size])

        # Determine the position of the final item following the page.
        if len(results) > len(self.page):
            has_following_position = True
            following_position = self._get_position_from_instance(results[-1], self.ordering)
        else:
            has_following_position = False
            following_position = None

        # If we have a reverse queryset, then the query ordering was in reverse
        # so we need to reverse the items again before returning them to the user.
        if reverse:
            self.page = list(reversed(self.page))

        if reverse:
            # Determine next and previous positions for reverse cursors.
            self.has_next = (current_position is not None) or (offset > 0)
            self.has_previous = has_following_position
            if self.has_next:
                self.next_position = current_position
            if self.has_previous:
                self.previous_position = following_position
        else:
            # Determine next and previous positions for forward cursors.
            self.has_next = has_following_position
            self.has_previous = (current_position is not None) or (offset > 0)
            if self.has_next:
                self.next_position = following_position
            if self.has_previous:
                self.previous_position = current_position

        return self.page

    def get_page_size(self, request):
        return self.page_size

    def get_next_token(self):
        if not self.has_next:
            return None

        if self.cursor and self.cursor.reverse and self.cursor.offset != 0:
            # If we're reversing direction and we have an offset cursor
            # then we cannot use the first position we find as a marker.
            compare = self._get_position_from_instance(self.page[-1], self.ordering)
        else:
            compare = self.next_position
        offset = 0

        for item in reversed(self.page):
            position = self._get_position_from_instance(item, self.ordering)
            if position != compare:
                # The item in this position and the item following it
                # have different positions. We can use this position as
                # our marker.
                break

            # The item in this position has the same position as the item
            # following it, we can't use it as a marker position, so increment
            # the offset and keep seeking to the previous item.
            compare = position
            offset += 1

        else:
            # There were no unique positions in the page.
            if not self.has_previous:
                # We are on the first page.
                # Our cursor will have an offset equal to the page size,
                # but no position to filter against yet.
                offset = self.page_size
                position = None
            elif self.cursor.reverse:
                # The change in direction will introduce a paging artifact,
                # where we end up skipping forward a few extra items.
                offset = 0
                position = self.previous_position
            else:
                # Use the position from the existing cursor and increment
                # it's offset by the page size.
                offset = self.cursor.offset + self.page_size
                position = self.previous_position

        cursor = Cursor(offset=offset, reverse=False, position=position)
        return self.encode_cursor(cursor)

    def get_previous_token(self):
        if not self.has_previous:
            return None

        if self.cursor and not self.cursor.reverse and self.cursor.offset != 0:
            # If we're reversing direction and we have an offset cursor
            # then we cannot use the first position we find as a marker.
            compare = self._get_position_from_instance(self.page[0], self.ordering)
        else:
            compare = self.previous_position
        offset = 0

        for item in self.page:
            position = self._get_position_from_instance(item, self.ordering)
            if position != compare:
                # The item in this position and the item following it
                # have different positions. We can use this position as
                # our marker.
                break

            # The item in this position has the same position as the item
            # following it, we can't use it as a marker position, so increment
            # the offset and keep seeking to the previous item.
            compare = position
            offset += 1

        else:
            # There were no unique positions in the page.
            if not self.has_next:
                # We are on the final page.
                # Our cursor will have an offset equal to the page size,
                # but no position to filter against yet.
                offset = self.page_size
                position = None
            elif self.cursor.reverse:
                # Use the position from the existing cursor and increment
                # it's offset by the page size.
                offset = self.cursor.offset + self.page_size
                position = self.next_position
            else:
                # The change in direction will introduce a paging artifact,
                # where we end up skipping back a few extra items.
                offset = 0
                position = self.next_position

        cursor = Cursor(offset=offset, reverse=True, position=position)
        return self.encode_cursor(cursor)

    def decode_cursor(self, encoded: str) -> Cursor:
        """
        Given a request with a cursor, return a `Cursor` instance.
        """
        # Determine if we have a cursor, and if so then decode it.
        if not encoded:
            return None

        try:
            querystring = b64decode(encoded.encode('ascii')).decode('ascii')
            tokens = parse_qs(querystring, keep_blank_values=True)

            offset = tokens.get('o', ['0'])[0]
            offset = _positive_int(offset, cutoff=self.offset_cutoff)

            reverse = tokens.get('r', ['0'])[0]
            reverse = bool(int(reverse))

            position = tokens.get('p', [None])[0]
        except (TypeError, ValueError):
            raise NotFound(self.invalid_cursor_message)

        return Cursor(offset=offset, reverse=reverse, position=position)

    def encode_cursor(self, cursor: Cursor) -> str:
        """
        Given a Cursor instance, return an url with encoded cursor.
        """
        tokens = {}
        if cursor.offset != 0:
            tokens['o'] = str(cursor.offset)
        if cursor.reverse:
            tokens['r'] = '1'
        if cursor.position is not None:
            tokens['p'] = cursor.position

        querystring = urlencode(tokens, doseq=True)
        encoded = b64encode(querystring.encode('ascii')).decode('ascii')
        return encoded

    def _get_position_from_instance(self, instance, ordering):
        field = ordering[0]['field']
        if isinstance(instance, dict):
            attr = instance[field]
        else:
            attr = getattr(instance, field)
        return str(attr)