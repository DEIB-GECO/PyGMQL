pyType_to_scalaType = {
    "object": "string",
    "int64": "long",
    "float64": "double",
    "int32": "integer",
    "float32": "double"
}

schema_header = "<?xml version='1.0' encoding='UTF-8'?>\n"
schema_collection_template = "<gmqlSchemaCollection name= \"{}\" xmlns=\"http://genomic.elet.polimi.it/entities\">\n"
schema_dataset_type_template = "\t<gmqlSchema type=\"{}\" coordinate_system=\"{}\">\n"
schema_field_template = "\t\t<field type=\"{}\">{}</field>\n"
schema_dataset_end = "\t</gmqlSchema>\n"
schema_collection_end = "</gmqlSchemaCollection>"
