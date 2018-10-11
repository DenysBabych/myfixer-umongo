import decimal
from calendar import timegm
from datetime import datetime, date
from dateutil.tz import tzutc

from marshmallow import ValidationError, Schema as MaSchema, missing, class_registry, utils
from marshmallow import fields as ma_fields, validates_schema
from marshmallow.base import SchemaABC
from marshmallow.compat import basestring
from marshmallow.fields import _RECURSIVE_NESTED
import bson

from .i18n import gettext as _


__all__ = (
    'schema_validator_check_unknown_fields',
    'schema_from_umongo_get_attribute',
    'SchemaFromUmongo',

    'StrictDateTime',
    'Timestamp',
    'ObjectId',
    'Reference',
    'GenericReference'
)


# Bonus: schema helpers !
UNKNOWN_FIELD_ERROR = _('Unknown field.')


def schema_validator_check_unknown_fields(self, data, original_data):
    """
    Schema validator, raise ValidationError for unknown fields in a
    marshmallow schema.

    example::

        class MySchema(marshsmallow.Schema):
            # method's name is not important
            __check_unknown_fields = validates_schema(pass_original=True)(
                schema_validator_check_unknown_fields)

            # Define the rest of your schema
            ...

    ..note:: Unknown fields with `missing` value will be ignored
    """
    # Just skip if dummy data have been passed to the schema
    if not isinstance(original_data, dict):
        return
    loadable_fields = [k for k, v in self.fields.items() if not v.dump_only]
    unknown_fields = {key for key, value in original_data.items()
                      if value is not missing and key not in loadable_fields}
    if unknown_fields:
        raise ValidationError(UNKNOWN_FIELD_ERROR, unknown_fields)


def schema_from_umongo_get_attribute(self, attr, obj, default):
    """
    Overwrite default `Schema.get_attribute` method by this one to access
        umongo missing fields instead of returning `None`.

    example::

        class MySchema(marshsmallow.Schema):
            get_attribute = schema_from_umongo_get_attribute

            # Define the rest of your schema
            ...

    """
    ret = MaSchema.get_attribute(self, attr, obj, default)
    if ret is None and ret is not default and attr in obj.schema.fields:
        raw_ret = obj._data.get(attr)
        return default if raw_ret is missing else raw_ret
    else:
        return ret


class SchemaFromUmongo(MaSchema):
    """
    Custom :class:`marshmallow.Schema` subclass providing unknown fields
    checking and custom get_attribute for umongo documents.

    .. note: It is not mandatory to use this schema with umongo document.
        This is just a helper providing usefull behaviors.
    """

    __check_unknown_fields = validates_schema(pass_original=True)(
        schema_validator_check_unknown_fields)
    get_attribute = schema_from_umongo_get_attribute


# Bonus: new fields !

class DateTime(ma_fields.DateTime):
    """
    Marshmallow DateTime field
    """

    def _deserialize(self, value, attr, data):
        if not isinstance(value, datetime):
            if isinstance(value, date):
                value = datetime.combine(value, datetime.min.time())
            else:
                value = super()._deserialize(value, attr, data)
        return value


class StrictDateTime(ma_fields.DateTime):
    """
    Marshmallow DateTime field with extra parameter to control
    whether dates should be loaded as tz_aware or not
    """

    def __init__(self, load_as_tz_aware=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.load_as_tz_aware = load_as_tz_aware

    def _deserialize(self, value, attr, data):
        date = super()._deserialize(value, attr, data)
        return self._set_tz_awareness(date)

    def _set_tz_awareness(self, date):
        if self.load_as_tz_aware:
            # If datetime is TZ naive, set UTC timezone
            if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
                date = date.replace(tzinfo=tzutc())
        else:
            # If datetime is TZ aware, convert it to UTC and remove TZ info
            if date.tzinfo is not None and date.tzinfo.utcoffset(date) is not None:
                date = date.astimezone(tzutc())
            date = date.replace(tzinfo=None)
        return date


class Timestamp(ma_fields.Integer):
    """
    Marshmallow Timestamp field
    """
    default_error_messages = {
        'invalid': 'Timestamp must be integer.'
    }

    def __init__(self, auto_now=False, **kwargs):
        super().__init__(**kwargs)
        if auto_now:
            self.missing = datetime.utcnow
            self.dump_only = True

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return timegm(value.utctimetuple())

    def _deserialize(self, value, attr, data):
        return datetime.utcfromtimestamp(value)


class Decimal(ma_fields.Decimal):
    """
    Marshmallow field for :class:`decimal.Decimal`
    """

    def __init__(self, places=None, rounding=None, allow_nan=False, as_string=False, **kwargs):
        if places is not None and not isinstance(places, decimal.Decimal):
            places = decimal.Decimal((0, (1,), -places))
        self.places = places
        self.rounding = rounding
        self.allow_nan = allow_nan
        self.as_string = as_string
        super(ma_fields.Number, self).__init__(as_string=as_string, **kwargs)


class Float(ma_fields.Float):
    """
    Marshmallow float field
    """

    def __init__(self, places=None, as_string=False, **kwargs):
        self.places = places
        self.as_string = as_string
        super().__init__(as_string=as_string, **kwargs)

    # override Number
    def _format_num(self, value):
        if value is None:
            return None

        value = self.num_type(value)
        if self.places is not None:
            value = round(value, self.places)
        return value


class ObjectId(ma_fields.Field):
    """
    Marshmallow field for :class:`bson.ObjectId`
    """

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return str(value)

    def _deserialize(self, value, attr, data):
        try:
            return bson.ObjectId(value)
        except (bson.errors.InvalidId, TypeError):
            raise ValidationError(_('Invalid ObjectId.'))


class Reference(ma_fields.Field):
    """
    Marshmallow field for :class:`umongo.fields.ReferenceField`
    """

    def __init__(self, nested, exclude=tuple(), only=None, mongo_world=False, **kwargs):
        self.nested = nested
        self.only = only
        self.exclude = exclude
        self.many = kwargs.get('many', False)
        self.mongo_world = mongo_world
        self.__schema = None  # Cached Schema instance
        self.__updated_fields = False
        super().__init__(**kwargs)

    @property
    def schema(self):
        """The nested Schema object.

        .. versionchanged:: 1.0.0
            Renamed from `serializer` to `schema`
        """
        if not self.__schema:
            # Ensure that only parameter is a tuple
            if isinstance(self.only, basestring):
                only = (self.only,)
            else:
                only = self.only

            # Inherit context from parent.
            context = getattr(self.parent, 'context', {})
            if isinstance(self.nested, SchemaABC):
                self.__schema = self.nested
                self.__schema.context.update(context)
            elif isinstance(self.nested, type) and \
                    issubclass(self.nested, SchemaABC):
                self.__schema = self.nested(many=self.many,
                        only=only, exclude=self.exclude, context=context,
                        load_only=self._nested_normalized_option('load_only'),
                        dump_only=self._nested_normalized_option('dump_only'))
            elif isinstance(self.nested, basestring):
                if self.nested == _RECURSIVE_NESTED:
                    parent_class = self.parent.__class__
                    self.__schema = parent_class(many=self.many, only=only,
                            exclude=self.exclude, context=context,
                            load_only=self._nested_normalized_option('load_only'),
                            dump_only=self._nested_normalized_option('dump_only'))
                else:
                    schema_class = class_registry.get_class(self.nested)
                    self.__schema = schema_class(many=self.many,
                            only=only, exclude=self.exclude, context=context,
                            load_only=self._nested_normalized_option('load_only'),
                            dump_only=self._nested_normalized_option('dump_only'))
            else:
                raise ValueError('Nested fields must be passed a '
                                 'Schema, not {0}.'.format(self.nested.__class__))
            self.__schema.ordered = getattr(self.parent, 'ordered', False)
        return self.__schema

    def _nested_normalized_option(self, option_name):
        nested_field = '%s.' % self.name
        return [field.split(nested_field, 1)[1]
                for field in getattr(self.root, option_name, set())
                if field.startswith(nested_field)]

    def _serialize(self, nested_obj, attr, obj):
        # Load up the schema first. This allows a RegistryError to be raised
        # if an invalid schema name was passed
        if nested_obj is None:
            return None
        elif self.mongo_world:
            # In mongo world, value is a regular ObjectId
            return str(nested_obj)

        if getattr(nested_obj, '_document', None):
            nested_obj = nested_obj._document
        else:
            return str(nested_obj.pk)

        schema = self.schema
        if not self.__updated_fields:
            schema._update_fields(obj=nested_obj, many=self.many)
            self.__updated_fields = True
        ret, errors = schema.dump(nested_obj, many=self.many,
                update_fields=not self.__updated_fields)
        if isinstance(self.only, basestring):  # self.only is a field name
            only_field = self.schema.fields[self.only]
            key = ''.join([self.schema.prefix or '', only_field.dump_to or self.only])
            if self.many:
                return utils.pluck(ret, key=key)
            else:
                return ret[key]
        if errors:
            raise ValidationError(errors, data=ret)
        return ret

    def _deserialize(self, value, attr, data):
        try:
            return bson.ObjectId(value)
        except (bson.errors.InvalidId, TypeError):
            raise ValidationError(_('Invalid ObjectId.'))


class GenericReference(ma_fields.Field):
    """
    Marshmallow field for :class:`umongo.fields.GenericReferenceField`
    """

    def __init__(self, *args, mongo_world=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_world = mongo_world

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        if self.mongo_world:
            # In mongo world, value a dict of cls and id
            return {'id': str(value['_id']), 'cls': value['_cls']}
        else:
            # In OO world, value is a :class:`umongo.data_object.Reference`
            return {'id': str(value.pk), 'cls': value.document_cls.__name__}

    def _deserialize(self, value, attr, data):
        if not isinstance(value, dict):
            raise ValidationError(_("Invalid value for generic reference field."))
        if value.keys() != {'cls', 'id'}:
            raise ValidationError(_("Generic reference must have `id` and `cls` fields."))
        try:
            _id = bson.ObjectId(value['id'])
        except ValueError:
            raise ValidationError(_("Invalid `id` field."))
        if self.mongo_world:
            return {'_cls': value['cls'], '_id': _id}
        else:
            return {'cls': value['cls'], 'id': _id}
