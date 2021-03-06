from datetime import datetime

from marshmallow import ValidationError, missing
from marshmallow import fields as ma_fields
from bson import DBRef, ObjectId, decimal128

# from .registerer import retrieve_document
from .exceptions import NotRegisteredDocumentError
from .template import get_template
from .data_objects import Reference, List, Dict
from . import marshmallow_bonus as ma_bonus_fields
from .abstract import BaseField
from .i18n import gettext as _


__all__ = (
    # 'RawField',
    'DictField',
    'ListField',
    'StringField',
    'UUIDField',
    'NumberField',
    'IntegerField',
    'DecimalField',
    'BooleanField',
    'FormattedStringField',
    'FloatField',
    'DateTimeField',
    # 'TimeField',
    # 'DateField',
    # 'TimeDeltaField',
    'UrlField',
    'URLField',
    'EmailField',
    'StrField',
    'BoolField',
    'IntField',
    'ConstantField',
    'StrictDateTimeField',
    'TimestampField',
    'ObjectIdField',
    'ReferenceField',
    'GenericReferenceField',
    'EmbeddedField'
)


# Republish supported marshmallow fields


# class RawField(BaseField, ma_fields.Raw):
#     pass


class DictField(BaseField, ma_fields.Dict):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('default', {})
        kwargs.setdefault('missing', Dict)
        super().__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data):
        value = super()._deserialize(value, attr, data)
        return Dict(value)

    def _serialize_to_mongo(self, obj):
        if not obj:
            return missing
        return dict(obj)

    def _deserialize_from_mongo(self, value, **kwargs):
        if value:
            return Dict(value)
        else:
            return Dict()

    def translate_query(self, key, query):
        keys = key.split('.')
        self.attribute or keys[0] + '.' + '.'.join(keys[1:])
        return {self.attribute or key: query}


class ListField(BaseField, ma_fields.List):

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('default', [])
        kwargs.setdefault('missing', lambda: List(self.container))
        super().__init__(*args, **kwargs)

    def _deserialize(self, value, attr, data):
        return List(self.container, super()._deserialize(value, attr, data))

    def _serialize_to_mongo(self, obj):
        if not obj:
            return missing
        return [self.container.serialize_to_mongo(each) for each in obj]

    def _deserialize_from_mongo(self, value, parent_instance=None, **kwargs):
        if value:
            return List(self.container, [self.container.deserialize_from_mongo(each, parent_instance=parent_instance)
                                         for each in value])
        else:
            return List(self.container)

    def map_to_field(self, mongo_path, path, func):
        """Apply a function to every field in the schema
        """
        func(mongo_path, path, self.container)
        if hasattr(self.container, 'map_to_field'):
            self.container.map_to_field(mongo_path, path, func)

    def as_marshmallow_field(self, params=None, mongo_world=False):
        # Overwrite default `as_marshmallow_field` to handle deserialization
        # difference (`_id` vs `id`)
        kwargs = self._extract_marshmallow_field_params(mongo_world)
        if params:
            kwargs.update(params)
        return ma_fields.List(self.container.as_marshmallow_field(
            mongo_world=mongo_world), **kwargs)

    def _required_validate(self, value):
        if value is missing or not hasattr(self.container, '_required_validate'):
            return
        required_validate = self.container._required_validate
        errors = {}
        for i, sub_value in enumerate(value):
            try:
                required_validate(sub_value)
            except ValidationError as exc:
                errors[i] = exc.messages
        if errors:
            raise ValidationError(errors)


class StringField(BaseField, ma_fields.String):
    pass


class UUIDField(BaseField, ma_fields.UUID):
    pass


class NumberField(BaseField, ma_fields.Number):
    pass


class IntegerField(BaseField, ma_fields.Integer):
    pass


class DecimalField(BaseField, ma_bonus_fields.Decimal):
    marshmallow_field_params = ('default', 'load_from', 'validate', 'required',  'allow_none',
                                'load_only', 'dump_only', 'missing', 'error_messages',
                                'places', 'rounding', 'allow_nan', 'as_string')

    def _serialize_to_mongo(self, obj):
        if not isinstance(obj, decimal128.Decimal128):
            obj = decimal128.Decimal128(obj)
        return obj

    def _deserialize_from_mongo(self, value, **kwargs):
        if isinstance(value, decimal128.Decimal128):
            value = value.to_decimal()
        return value


class BooleanField(BaseField, ma_fields.Boolean):
    pass


class FormattedStringField(BaseField, ma_fields.FormattedString):
    pass


class FloatField(BaseField, ma_bonus_fields.Float):
    marshmallow_field_params = ('default', 'load_from', 'validate', 'required',  'allow_none',
                                'load_only', 'dump_only', 'missing', 'error_messages',
                                'places', 'as_string')


class DateTimeField(BaseField, ma_bonus_fields.DateTime):
    marshmallow_field_params = ('default', 'load_from', 'validate', 'required', 'allow_none',
                                'load_only', 'dump_only', 'missing', 'error_messages', 'format')

    def __init__(self, format=None, auto_now=False, **kwargs):
        super().__init__(format=format, **kwargs)
        self.format = format
        if auto_now:
            self.missing = datetime.utcnow
            self.dump_only = True


class LocalDateTimeField(BaseField, ma_fields.LocalDateTime):

    def _deserialize(self, value, attr, data):
        if isinstance(value, datetime):
            return value
        return super()._deserialize(value, attr, data)


# class TimeField(BaseField, ma_fields.Time):
#     pass


# class DateField(BaseField, ma_fields.Date):
#     pass


# class TimeDeltaField(BaseField, ma_fields.TimeDelta):
#     pass


class UrlField(BaseField, ma_fields.Url):
    pass


class EmailField(BaseField, ma_fields.Email):
    pass


class ConstantField(BaseField, ma_fields.Constant):
    pass


# Aliases
URLField = UrlField
StrField = StringField
BoolField = BooleanField
IntField = IntegerField


# Bonus: new fields !

class StrictDateTimeField(BaseField, ma_bonus_fields.StrictDateTime):

    def _deserialize(self, value, attr, data):
        if isinstance(value, datetime):
            return self._set_tz_awareness(value)
        return super()._deserialize(value, attr, data)

    def _deserialize_from_mongo(self, value, **kwargs):
        return self._set_tz_awareness(value)


class TimestampField(BaseField, ma_bonus_fields.Timestamp):
    pass


class ObjectIdField(BaseField, ma_bonus_fields.ObjectId):
    def _deserialize_from_mongo(self, value, **kwargs):
        if not isinstance(value, ObjectId):
            value = ObjectId(value)
        return value


class ReferenceField(BaseField, ma_bonus_fields.Reference):

    def __init__(self, document, *args, reference_cls=Reference, generate_reference_scheme=True, **kwargs):
        """
        :param document: Can be a :class:`umongo.embedded_document.DocumentTemplate`,
            another instance's :class:`umongo.embedded_document.DocumentImplementation` or
            the embedded document class name.
        """
        super().__init__(None, *args, **kwargs)
        # TODO : check document_cls is implementation or string
        self.reference_cls = reference_cls
        self.generate_reference_scheme = generate_reference_scheme
        # Can be the Template, Template's name or another Implementation
        if not isinstance(document, str):
            self.document = get_template(document)
        else:
            self.document = document
        self._document_cls = None
        # Avoid importing multiple times
        from .document import DocumentImplementation
        self._document_implementation_cls = DocumentImplementation

    @property
    def document_cls(self):
        """
        Return the instance's :class:`umongo.embedded_document.DocumentImplementation`
        implementing the `document` attribute.
        """
        if not self._document_cls:
            self._document_cls = self.instance.retrieve_document(self.document)
        return self._document_cls

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        if isinstance(value, DBRef):
            if self._document_cls.collection.name != value.collection:
                raise ValidationError(_("DBRef must be on collection `{collection}`.").format(
                    self._document_cls.collection.name))
            value = value.id
        elif isinstance(value, Reference):
            if value.document_cls != self.document_cls:
                raise ValidationError(_("`{document}` reference expected.").format(
                    document=self.document_cls.__name__))
            if type(value) is not self.reference_cls:
                value = self.reference_cls(value.document_cls, value.pk)
            return value
        elif isinstance(value, self.document_cls):
            if not value.is_created:
                raise ValidationError(
                    _("Cannot reference a document that has not been created yet."))
            return self._deserialize_from_mongo(value)
        elif isinstance(value, self._document_implementation_cls):
            raise ValidationError(_("`{document}` reference expected.").format(
                document=self.document_cls.__name__))
        value = super()._deserialize(value, attr, data)
        # `value` is similar to data received from the database so we
        # can use `_deserialize_from_mongo` to finish the deserialization
        return self._deserialize_from_mongo(value)

    def _serialize_to_mongo(self, obj):
        return obj.pk

    def _deserialize_from_mongo(self, value, **kwargs):
        # When this method is called from `_deserialize`, `value` can be
        # already deserialized, in such a case do nothing.
        if isinstance(value, self.reference_cls):
            return value

        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        if isinstance(value, dict):
            value = self.document_cls.build_from_mongo(value, use_cls=True)

        if isinstance(value, self.document_cls):
            reference = self.reference_cls(self.document_cls, value.pk)
            reference._document = value
            return reference
        elif not isinstance(value, ObjectId):
            value = ObjectId(value)
        return self.reference_cls(self.document_cls, value)

    def as_marshmallow_field(self, params=None, mongo_world=False):
        # Overwrite default `as_marshmallow_field` to handle nesting
        kwargs = self._extract_marshmallow_field_params(mongo_world)
        if params:
            reference_params = params.pop('params')
            kwargs.update(params)
        else:
            reference_params = None
        if self.generate_reference_scheme:
            reference_ma_schema = self.document_cls.schema.as_marshmallow_schema(
                params=reference_params, mongo_world=mongo_world)
        else:
            reference_ma_schema = None
        schema = ma_bonus_fields.Reference(reference_ma_schema, mongo_world=mongo_world, **kwargs)
        return schema


class GenericReferenceField(BaseField, ma_bonus_fields.GenericReference):

    def __init__(self, *args, reference_cls=Reference, **kwargs):
        super().__init__(*args, **kwargs)
        self.reference_cls = reference_cls
        # Avoid importing multiple times
        from .document import DocumentImplementation
        self._document_implementation_cls = DocumentImplementation

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return {'id': str(value.pk), 'cls': value.document_cls.__name__}

    def _deserialize(self, value, attr, data):
        if value is None:
            return None
        if isinstance(value, Reference):
            if type(value) is not self.reference_cls:
                value = self.reference_cls(value.document_cls, value.pk)
            return value
        elif isinstance(value, self._document_implementation_cls):
            if not value.is_created:
                raise ValidationError(
                    _("Cannot reference a document that has not been created yet."))
            return self.reference_cls(value.__class__, value.pk)
        elif isinstance(value, dict):
            if value.keys() != {'cls', 'id'}:
                raise ValidationError(_("Generic reference must have `id` and `cls` fields."))
            try:
                _id = ObjectId(value['id'])
            except ValueError:
                raise ValidationError(_("Invalid `id` field."))
            return self._deserialize_from_mongo({
                '_cls': value['cls'],
                '_id': _id
            })
        else:
            raise ValidationError(_("Invalid value for generic reference field."))

    def _serialize_to_mongo(self, obj):
        return {'_id': obj.pk, '_cls': obj.document_cls.__name__}

    def _deserialize_from_mongo(self, value, **kwargs):
        # When this method is called from `_deserialize`, `value` can be
        # already deserialized, in such a case do nothing.
        if isinstance(value, self.reference_cls):
            return value
        try:
            document_cls = self.instance.retrieve_document(value['_cls'])
        except NotRegisteredDocumentError:
            raise ValidationError(_('Unknown document `{document}`.').format(
                document=value['_cls']))
        return self.reference_cls(document_cls, value['_id'])

    def as_marshmallow_field(self, params=None, mongo_world=False):
        # Overwrite default `as_marshmallow_field` to handle deserialization
        # difference (`_id` vs `id`)
        kwargs = self._extract_marshmallow_field_params(mongo_world)
        if params:
            kwargs.update(params)
        return ma_bonus_fields.GenericReference(mongo_world=mongo_world, **kwargs)


class EmbeddedField(BaseField, ma_fields.Nested):

    def __init__(self, embedded_document, *args, **kwargs):
        """
        :param embedded_document: Can be a
            :class:`umongo.embedded_document.EmbeddedDocumentTemplate`,
            another instance's :class:`umongo.embedded_document.EmbeddedDocumentImplementation`
            or the embedded document class name.
        """
        # Don't need to pass `nested` attribute given it is overloaded
        super().__init__(None, *args, **kwargs)
        # Try to retrieve the template if possible for consistency
        if not isinstance(embedded_document, str):
            self.embedded_document = get_template(embedded_document)
        else:
            self.embedded_document = embedded_document
        self._embedded_document_cls = None

    @property
    def nested(self):
        # Overload `nested` attribute to be able to fetch it lazily
        return self.embedded_document_cls.Schema

    @nested.setter
    def nested(self, value):
        pass

    @property
    def embedded_document_cls(self):
        """
        Return the instance's :class:`umongo.embedded_document.EmbeddedDocumentImplementation`
        implementing the `embedded_document` attribute.
        """
        if not self._embedded_document_cls:
            self._embedded_document_cls = self.instance.retrieve_embedded_document(
                self.embedded_document)
        return self._embedded_document_cls

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        return value.dump()

    def _deserialize(self, value, attr, data):
        embedded_document_cls = self.embedded_document_cls
        if isinstance(value, embedded_document_cls):
            return value
        # Handle inheritance deserialization here using `cls` field as hint
        if embedded_document_cls.opts.offspring and isinstance(value, dict) and 'cls' in value:
            to_use_cls_name = value.pop('cls')
            if not any(o for o in embedded_document_cls.opts.offspring
                       if o.__name__ == to_use_cls_name):
                raise ValidationError(_('Unknown document `{document}`.').format(
                    document=to_use_cls_name))
            try:
                to_use_cls = embedded_document_cls.opts.instance.retrieve_embedded_document(
                    to_use_cls_name)
            except NotRegisteredDocumentError as e:
                raise ValidationError(str(e))
            return to_use_cls(**value)
        else:
            # `Nested._deserialize` calls schema.load without partial=True
            data, errors = self.schema.load(value, partial=True)
            if errors:
                raise ValidationError(errors, data=data)
            return self._deserialize_from_mongo(data)

    def _serialize_to_mongo(self, obj):
        value = obj.to_mongo()
        return value if value is not None or self.allow_none else missing

    def _deserialize_from_mongo(self, value, parent_instance=None, **kwargs):
        # When this method is called from `_deserialize`, `value` can be
        # already deserialized, in such a case do nothing.
        if isinstance(value, self.embedded_document_cls):
            return value
        return self.embedded_document_cls.build_from_mongo(value, parent_instance=parent_instance)

    def _validate_missing(self, value):
        # Overload default to handle recursive check
        super()._validate_missing(value)
        errors = {}
        if value is missing:
            def get_sub_value(key):
                return missing
        elif isinstance(value, dict):
            # value is a dict for deserialization
            def get_sub_value(key):
                return value.get(key, missing)
        elif isinstance(value, self.embedded_document_cls):
            # value is a valid EmbeddedDocument
            def get_sub_value(key):
                return value._data.get(key)
        else:
            # value is invalid, just return and let `_deserialize`
            # raises an error about this
            return
        for name, field in self.embedded_document_cls.schema.fields.items():
            sub_value = get_sub_value(name)
            # `_validate_missing` doesn't check for required fields here, so we
            # can safely skip missing values
            if sub_value is missing:
                continue
            try:
                field._validate_missing(sub_value)
            except ValidationError as ve:
                errors[name] = ve.messages
        if errors:
            raise ValidationError(errors)

    def map_to_field(self, mongo_path, path, func):
        """Apply a function to every field in the schema"""
        for name, field in self.embedded_document_cls.schema.fields.items():
            cur_path = '%s.%s' % (path, name)
            cur_mongo_path = '%s.%s' % (mongo_path, field.attribute or name)
            func(cur_mongo_path, cur_path, field)
            if hasattr(field, 'map_to_field'):
                field.map_to_field(cur_mongo_path, cur_path, func)

    def as_marshmallow_field(self, params=None, mongo_world=False):
        # Overwrite default `as_marshmallow_field` to handle nesting
        kwargs = self._extract_marshmallow_field_params(mongo_world)
        if params:
            nested_params = params.pop('params')
            kwargs.update(params)
        else:
            nested_params = None
        nested_ma_schema = self._embedded_document_cls.schema.as_marshmallow_schema(
            params=nested_params, mongo_world=mongo_world)
        return ma_fields.Nested(nested_ma_schema, **kwargs)

    def _required_validate(self, value):
        if value is not missing and not (value is None and self.allow_none):
            value.required_validate()
