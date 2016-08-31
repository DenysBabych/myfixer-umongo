import pytest
from datetime import datetime
from bson import ObjectId, DBRef
import marshmallow

from umongo import Document, EmbeddedDocument, Schema, fields, exceptions, set_gettext, validate
from umongo import marshmallow_bonus_fields as ma_bonus_fields
from umongo.abstract import BaseField, BaseSchema

from .common import BaseTest


class TestMarshmallow(BaseTest):

    def teardown_method(self, method):
        # Reset i18n config before each test
        set_gettext(None)

    def setup(self):
        super().setup()

        class User(Document):
            name = fields.StrField()
            birthday = fields.DateTimeField()

            class Meta:
                allow_inheritance = True

        self.User = self.instance.register(User)

    def test_by_field(self):
        ma_name_field = self.User.schema.fields['name'].as_marshmallow_field()
        assert isinstance(ma_name_field, marshmallow.fields.Field)
        assert not isinstance(ma_name_field, BaseField)

    def test_by_schema(self):
        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        assert issubclass(ma_schema_cls, marshmallow.Schema)
        assert not issubclass(ma_schema_cls, BaseSchema)

    def test_custom_base_schema(self):

        class MyBaseSchema(marshmallow.Schema):
            name = marshmallow.fields.Int()
            age = marshmallow.fields.Int()

        ma_schema_cls = self.User.schema.as_marshmallow_schema(base_schema_cls=MyBaseSchema)
        assert issubclass(ma_schema_cls, MyBaseSchema)

        schema = ma_schema_cls()
        ret = schema.dump({'name': "42", 'age': 42, 'dummy': False})
        assert not ret.errors
        assert ret.data == {'name': "42", 'age': 42}

    def test_customize_params(self):
        ma_field = self.User.schema.fields['name'].as_marshmallow_field(params={'load_only': True})
        assert ma_field.load_only is True

        ma_schema_cls = self.User.schema.as_marshmallow_schema(params={'name': {'load_only': True}})
        schema = ma_schema_cls()
        ret = schema.dump({'name': "42", 'birthday': datetime(1990, 10, 23), 'dummy': False})
        assert not ret.errors
        assert ret.data == {'birthday': '1990-10-23T00:00:00+00:00'}

    def test_keep_attributes(self):
        @self.instance.register
        class Vehicle(Document):
            pass
        # TODO

    def test_keep_validators(self):
        @self.instance.register
        class WithMailUser(self.User):
            email = fields.StrField(validate=[validate.Email(), validate.Length(max=100)])
            number_of_legs = fields.IntField(validate=[validate.OneOf([0, 1, 2])])

        ma_schema_cls = WithMailUser.schema.as_marshmallow_schema()
        schema = ma_schema_cls()

        ret = schema.load({'email': 'a' * 100 + '@user.com', 'number_of_legs': 4})
        assert ret.errors == {'email': ['Longer than maximum length 100.'],
                              'number_of_legs': ['Not a valid choice.']}

        ret = schema.load({'email': 'user@user.com', 'number_of_legs': 2})
        assert not ret.errors
        assert ret.data == {'email': 'user@user.com', 'number_of_legs': 2}

    def test_inheritance(self):
        @self.instance.register
        class AdvancedUser(self.User):
            name = fields.StrField(default='1337')
            is_left_handed = fields.BooleanField()

        ma_schema_cls = AdvancedUser.schema.as_marshmallow_schema()
        schema = ma_schema_cls()

        ret = schema.dump({'is_left_handed': True})
        assert not ret.errors
        assert ret.data == {'name': '1337', 'is_left_handed': True}

    def test_to_mongo(self):
        @self.instance.register
        class Dog(Document):
            name = fields.StrField(attribute='_id', required=True)
            age = fields.IntField()

        payload = {'name': 'Scruffy', 'age': 2}
        ma_schema_cls = Dog.schema.as_marshmallow_schema()
        ma_mongo_schema_cls = Dog.schema.as_marshmallow_schema(mongo_world=True)

        ret = ma_schema_cls().load(payload)
        assert not ret.errors
        assert ret.data == {'name': 'Scruffy', 'age': 2}
        assert ma_schema_cls().dump(ret.data).data == payload

        ret = ma_mongo_schema_cls().load(payload)
        assert not ret.errors
        assert ret.data == {'_id': 'Scruffy', 'age': 2}
        assert ma_mongo_schema_cls().dump(ret.data).data == payload

    def test_i18n(self):
        # i18n support should be kept, because it's pretty cool to have this !
        def my_gettext(message):
            return 'OMG !!! ' + message

        set_gettext(my_gettext)

        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        ret = ma_schema_cls().load({'name': 'John', 'birthday': 'not_a_date', 'dummy_field': 'dummy'})
        assert ret.errors == {'birthday': ['OMG !!! Not a valid datetime.'],
                              '_schema': ['OMG !!! Unknown field name dummy_field.']}

    def test_unknow_fields_check(self):
        ma_schema_cls = self.User.schema.as_marshmallow_schema()
        ret = ma_schema_cls().load({'name': 'John', 'dummy_field': 'dummy'})
        assert ret.errors == {'_schema': ['Unknown field name dummy_field.']}

        ma_schema_cls = self.User.schema.as_marshmallow_schema(check_unknown_fields=False)
        ret = ma_schema_cls().load({'name': 'John', 'dummy_field': 'dummy'})
        assert not ret.errors
        assert ret.data == {'name': 'John'}

    def test_nested_field(self):
        @self.instance.register
        class Accessory(EmbeddedDocument):
            brief = fields.StrField(attribute='id', required=True)
            value = fields.IntField()

        @self.instance.register
        class Bag(Document):
            id = fields.EmbeddedField(Accessory, attribute='_id', required=True)
            content = fields.ListField(fields.EmbeddedField(Accessory))

        data = {
            'id': {'brief': 'sportbag', 'value': 100},
            'content': [{'brief': 'cellphone', 'value': 500}, {'brief': 'lighter', 'value': 2}]
        }
        # Here data is the same in both OO world and user world (no
        # ObjectId to str conversion needed for example)

        ma_schema = Bag.schema.as_marshmallow_schema()()
        ma_mongo_schema = Bag.schema.as_marshmallow_schema(mongo_world=True)()

        bag = Bag(**data)
        ret = ma_schema.dump(bag)
        assert not ret.errors
        assert ret.data == data
        ret = ma_schema.load(data)
        assert not ret.errors
        assert ret.data == data

        ret = ma_mongo_schema.dump(bag.to_mongo())
        assert not ret.errors
        assert ret.data == data
        ret = ma_mongo_schema.load(data)
        assert not ret.errors
        assert ret.data == bag.to_mongo()

    def test_marshmallow_bonus_fields(self):
        # Fields related to mongodb provided for marshmallow
        @self.instance.register
        class Doc(Document):
            id = fields.ObjectIdField(attribute='_id')
            ref = fields.ReferenceField('Doc')
            gen_ref = fields.GenericReferenceField()

        for name, field_cls in (
                ('id', ma_bonus_fields.ObjectId),
                ('ref', ma_bonus_fields.ObjectId),
                ('gen_ref', ma_bonus_fields.GenericReference)
                ):
            ma_field = Doc.schema.fields[name].as_marshmallow_field()
            assert isinstance(ma_field, field_cls)
            assert not isinstance(ma_field, BaseField)


        oo_data = {
            'id': ObjectId("57c1a71113adf27ab96b2c4f"),
            'ref': ObjectId("57c1a71113adf27ab96b2c4f"),
            "gen_ref": {'cls': 'Doc', 'id': ObjectId("57c1a71113adf27ab96b2c4f")}
        }
        serialized = {
            'id': "57c1a71113adf27ab96b2c4f",
            'ref': "57c1a71113adf27ab96b2c4f",
            "gen_ref": {'cls': 'Doc', 'id': "57c1a71113adf27ab96b2c4f"}
        }
        doc = Doc(**oo_data)
        mongo_data = doc.to_mongo()

        # schema to OO world
        ma_schema_cls = Doc.schema.as_marshmallow_schema()
        ma_schema = ma_schema_cls()
        ret = ma_schema.dump(doc)
        assert not ret.errors
        assert ret.data == serialized
        ret = ma_schema.load(serialized)
        assert not ret.errors
        assert ret.data == oo_data

        # schema to mongo world
        ma_mongo_schema_cls = Doc.schema.as_marshmallow_schema(mongo_world=True)
        ma_mongo_schema = ma_mongo_schema_cls()
        ret = ma_mongo_schema.dump(mongo_data)
        assert not ret.errors
        assert ret.data == serialized
        ret = ma_mongo_schema.load(serialized)
        assert ret.errors == {}
        assert ret.data == mongo_data
        # Cannot load mongo form
        ret = ma_mongo_schema.load({"gen_ref": {'_cls': 'Doc', '_id': "57c1a71113adf27ab96b2c4f"}})
        assert ret.errors == {'gen_ref': ['Generic reference must have `id` and `cls` fields.']}
