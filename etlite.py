from decimal import Decimal


def cast_list_to_class_datatype(list_of_values, datatype_class, in_place=False):
    # float_values = [float(i) for i in float_values] # 0.25 secs  (1,000,000 elements)

    if not in_place:
        new_list_of_values_as_class_datatype = list(map(datatype_class, list_of_values))  # 0.19 secs
        return new_list_of_values_as_class_datatype
    else:
        for index, value in enumerate(list_of_values):
            list_of_values[index] = datatype_class(value)  # 0.35 secs
        return list_of_values  # Now in datatype form


def list_to_floats(list_of_values, in_place=False):
    datatype = float

    return cast_list_to_class_datatype(list_of_values=list_of_values, datatype_class=datatype, in_place=in_place)


def list_to_decimals(list_of_values, in_place=False):
    datatype = Decimal

    return cast_list_to_class_datatype(list_of_values=list_of_values, datatype_class=datatype, in_place=in_place)


def list_to_strings(list_of_values, in_place=False):
    datatype = str

    return cast_list_to_class_datatype(list_of_values=list_of_values, datatype_class=datatype, in_place=in_place)


def list_to_integers(list_of_values, in_place=False):
    datatype = int

    return cast_list_to_class_datatype(list_of_values=list_of_values, datatype_class=datatype, in_place=in_place)


def list_to_booleans(list_of_values, in_place=False):
    datatype = bool

    return cast_list_to_class_datatype(list_of_values=list_of_values, datatype_class=datatype, in_place=in_place)


def list_to_dates():
    # TODO: Deal with this later
    pass


class DataSchema(object):

    def __init__(self):

        self.data_schema = {}
        self.field_names = {}
        self.static_schema = True
        self.numeric_types = (float, int, Decimal)
        self.decimal_types = (float, Decimal)
        self.string_types = [str]
        self.data_type_map = {
            str: "string",
            float: "float",
            Decimal: "decimal",
            int: "integer",
            bool: "boolean",
            list: "list",
            dict: "dictionary"}
        self.blank_values = [None, ""]
        self.unique_values_map = {}  # fld name  dict of values

    def define_field(self, name, data_type, label="", required=False, read_only=False, null=True, blank=False,
                     unique=False, choices="", default="", max_length=256, max_digits=14, decimal_places=2, min_value=0,
                     max_value=0):

        data_field_definition = {"name": name,
                                 "data_type": data_type,
                                 "default": default,
                                 "null": null,
                                 "blank": blank,
                                 "unique": unique,
                                 "read_only": read_only,
                                 "required": required}

        if label:
            data_field_definition["label"] = label
        else:
            data_field_definition["label"] = name

        if choices:
            data_field_definition["choices"] = choices
        else:
            data_field_definition["choices"] = None

        if data_type in self.string_types:
            data_field_definition["max_length"] = max_length

        if data_type in self.decimal_types:
            data_field_definition["decimal_places"] = decimal_places

        if data_type in self.numeric_types:
            data_field_definition["max_digits"] = max_digits
            data_field_definition["min_value"] = min_value
            data_field_definition["max_value"] = max_value

        self.field_names[name] = data_field_definition

    def define_fields(self, field_metadata, is_minimal_data=True):

        if is_minimal_data:
            for field in list(field_metadata.keys()):
                self.define_field(name=field, data_type=field_metadata[field])
        else:
            for field in list(field_metadata.keys()):
                metadata = field_metadata[field]
                self.define_field(**metadata)

    def validate_value(self, value, field_name):
        validation_data = {"data_type": isinstance(value, self.field_names[field_name]["data_type"])}

        if validation_data["data_type"] in self.string_types:
            validation_data["max_length"] = len(value) <= self.field_names["max_length"]

        if self.field_names[field_name]["choices"]:
            validation_data["choices"] = value in self.field_names[field_name]["choices"]

        if self.field_names[field_name]["unique"]:
            if value in self.unique_values_map[field_name]:
                validation_data["unique"] = False
            else:
                validation_data["unique"] = True

        # Check the data and make the final call.
        unique_booleans = set(validation_data.values())
        if False in unique_booleans:
            validation_data["is_valid"] = False
        else:
            validation_data["is_valid"] = True
        return validation_data

    def validate_row(self, row):

        row_field_names = list(row.keys())
        schema_field_names = list(self.field_names.keys())
        #  not right need to make a list of required flds and see if they exist

        validation_data = {}

        if row_field_names == schema_field_names:
            validation_data["field_names"] = True
        else:
            validation_data["field_names"] = False

        for field in row:
            self.validate_value(value=field, field_name=field)

        # if any data element is not valid; mark the whole row as bad


class Extract(object):

    def __init__(self, data_file, data_delimiter, database_name, database_url, database_port, database_query):

        self.data_file = data_file
        self.data_delimiter = data_delimiter
        self.database_name = database_name
        self.database_url = database_url
        self.database_port = database_port
        self.database_query = database_query

    def execute(self):
        pass  # returns a raw data dict


class Transform(object):

    def __init__(self, data_schema, data_source):

        # data schemas
        self.data_schema = data_schema  # {}
        self.static_schema = True

        # data for transform
        self.data_source = data_source  # []

        # built in transformations
        self.fields_to_remove = []
        self.fields_to_typecast = []
        self.fields_to_rename = {"old_name": "new_name"}
        self.field_values_to_remap = {"field_name_status": {0: "active", 1: "inactive"}}
        self.fields_to_add = []
        self.enable_custom_transformations = False

    def remove_fields(self, row_data):

        for field in self.fields_to_remove:
            row_data.pop(field, None)
        return row_data

    def rename_fields(self, row_data):

        for field in list(self.fields_to_rename.keys()):
            row_data[self.fields_to_rename[field]] = row_data.pop(field, None)
        return row_data

    def find_and_replace_values(self, row_data):

        for field in list(self.field_values_to_remap.keys()):
            field_value_map = self.field_values_to_remap[field]

            if row_data[field] in field_value_map:
                row_data[field] = field_value_map[row_data[field]]
            else:
                pass  # set to none or keep? user setting?
        return row_data

    def typecast_fields(self, row_data):

        for field in self.fields_to_typecast:
            datatype_to_cast = self.data_schema[field]["type"]
            row_data[field] = datatype_to_cast()
        return row_data

    def add_fields(self, row_data):

        for field in self.fields_to_add:
            row_data[field] = None
        return row_data

    def custom_transformations(self, row_data):
        # You need to override this!
        raise NotImplementedError

    def execute(self):

        transformed_data = []
        for raw_row in self.data_source:
            row = raw_row

            if self.fields_to_remove:
                row = self.remove_fields(row_data=row)
            if self.fields_to_rename:
                row = self.rename_fields(row_data=row)
            if self.fields_to_typecast:
                row = self.typecast_fields(row_data=row)
            if self.field_values_to_remap:
                row = self.find_and_replace_values(row_data=row)
            if self.fields_to_add:
                row = self.add_fields(row_data=row)
            if self.custom_transformations:
                row = self.custom_transformations(row_data=row)

            transformed_data.append(row)
        return transformed_data


class Load(object):

    def __init__(self, data_file, data_delimiter, database_name, database_url, database_port, database_query):

        self.data_file = data_file
        self.data_delimiter = data_delimiter
        self.database_name = database_name
        self.database_url = database_url
        self.database_port = database_port
        self.database_query = database_query

    def execute(self):
        pass  # loads into data source
    # if uniqe togther issue could use md5 hash and lookup dict/set


class ExtractTransformLoad(object):

    def __init__(self, name, extractor, transformer, loader):

        self.name = name
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self):

        self.extractor.execute()
        self.transformer.execute()
        self.loader.execute()

    def run_batch(self, size=100):
        pass

    def run_single(self):
        pass


class ExtractTransformLoadPipeline(object):

    def __init__(self, list_of_etl_classes):
        self.list_of_etl_classes = list_of_etl_classes

    def run_all(self):
        for etl in self.list_of_etl_classes:
            etl.run()
