#include <stdio.h>
#include <ctype.h>
#include <stdint.h>
#include <stdbool.h>
#include <limits.h>
#include <assert.h>
#include <avro.h>
#include <jansson.h>

#ifdef DEFLATE_CODEC
    #define QUICKSTOP_CODEC  "deflate"
#else
    #define QUICKSTOP_CODEC  "null"
#endif

#define RECORD_NAME_LEN     64
#define FIELD_NAME_LEN      64
#define TYPE_NAME_LEN       16

typedef struct FieldStruct_S {
    char name[FIELD_NAME_LEN];
    char type[TYPE_NAME_LEN];
} FieldStruct;

typedef struct RecordSchema_S {
    char name[RECORD_NAME_LEN];
    char *fields;
    int  num_fields;
} RecordSchema;

typedef struct SArguments_S {
    bool read_file;
    char *read_filename;
    uint64_t count;
    bool schema_only;
    bool write_file;
    char *write_filename;
    char *json_filename;
    char *data_filename;
} SArguments;

SArguments g_args = {
    false,          // read_file
    "",             // read_filename
    UINT64_MAX,       // count
    false,          // schema_only
    false,          // write_file
    "",             // write_filename
    "",             // json_filename
    "",             // data_filename
};


#define tstrncpy(dst, src, size) \
  do {                              \
    strncpy((dst), (src), (size));  \
    (dst)[(size)-1] = 0;            \
  } while (0)


static RecordSchema *parse_json_to_recordschema(json_t *element);
static void print_json_aux(json_t *element, int indent);

static void printHelp()
{
    char indent[10] = "        ";

    printf("\n");

    printf("%s\n\n", "avrotool usage:");
    printf("%s%s%s%s\n", indent, "-r\t", indent,
            "<avro filename>. print avro file's contents including schema and data.");
    printf("%s%s%s%s\n", indent, "-c\t", indent,
            "<count>. specify number of avro data to print.");
    printf("%s%s%s%s\n", indent, "-s\t", indent,
            "<avro filename>. print avro schema only.");
    printf("%s%s%s%s\n", indent, "-w\t", indent,
            "<avro filename>. specify avro filename to write.");
    printf("%s%s%s%s\n", indent, "-m\t", indent,
            "<json filename>. use json as schema to write data to avro file.");
    printf("%s%s%s%s\n", indent, "-d\t", indent,
            "<data filename>. use csv file as input data.");
    printf("%s%s%s%s\n", indent, "--help\t", indent,
            "Print command line arguments list info.");

    printf("\n");
}

static bool isStringNumber(char *input)
{
    int len = strlen(input);
    if (0 == len) {
        return false;
    }

    for (int i = 0; i < len; i++) {
        if (!isdigit(input[i]))
            return false;
    }

    return true;
}

static bool parse_args(int argc, char *argv[], SArguments *arguments)
{
    bool has_flags = true;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-r") == 0) {
            arguments->read_file = true;
            arguments->read_filename = argv[++i];
        } else if (strcmp(argv[i], "-s") == 0) {
            arguments->schema_only = true;
            arguments->read_filename = argv[++i];
        } else if (strcmp(argv[i], "-c") == 0) {
            if (isStringNumber(argv[i+1])) {
                arguments->count = atoi(argv[++i]);
            }
        } else if (strcmp(argv[i], "-w") == 0) {
            arguments->write_file = true;
            arguments->write_filename = argv[++i];
        } else if (strcmp(argv[i], "-m") == 0) {
            arguments->json_filename = argv[++i];
        } else if (strcmp(argv[i], "-d") == 0) {
            arguments->data_filename = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0) {
            printHelp();
            exit(0);
        } else {
            has_flags = false;
        }
    }

    return has_flags;
}


static void print_json_indent(int indent) {
    int i;
    for (i = 0; i < indent; i++) {
        putchar(' ');
    }
}

const char *json_plural(size_t count) { return count == 1 ? "" : "s"; }

static void print_json_object(json_t *element, int indent) {
    size_t size;
    const char *key;
    json_t *value;

    print_json_indent(indent);
    size = json_object_size(element);

    printf("JSON Object of %lld pair%s:\n", (long long)size, json_plural(size));
    json_object_foreach(element, key, value) {
        print_json_indent(indent + 2);
        printf("JSON Key: \"%s\"\n", key);
        print_json_aux(value, indent + 2);
    }
}

static void print_json_array(json_t *element, int indent) {
    size_t i;
    size_t size = json_array_size(element);
    print_json_indent(indent);

    printf("JSON Array of %lld element%s:\n", (long long)size, json_plural(size));
    for (i = 0; i < size; i++) {
        print_json_aux(json_array_get(element, i), indent + 2);
    }
}

static void print_json_string(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON String: \"%s\"\n", json_string_value(element));
}

static void print_json_integer(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Integer: \"%" JSON_INTEGER_FORMAT "\"\n", json_integer_value(element));
}

static void print_json_real(json_t *element, int indent) {
    print_json_indent(indent);
    printf("JSON Real: %f\n", json_real_value(element));
}

static void print_json_true(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON True\n");
}

static void print_json_false(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON False\n");
}

static void print_json_null(json_t *element, int indent) {
    (void)element;
    print_json_indent(indent);
    printf("JSON Null\n");
}

static void print_json_aux(json_t *element, int indent)
{
    switch(json_typeof(element)) {
        case JSON_OBJECT:
            print_json_object(element, indent);
            break;

        case JSON_ARRAY:
            print_json_array(element, indent);
            break;

        case JSON_STRING:
            print_json_string(element, indent);
            break;

        case JSON_INTEGER:
            print_json_integer(element, indent);
            break;

        case JSON_REAL:
            print_json_real(element, indent);
            break;

        case JSON_TRUE:
            print_json_true(element, indent);
            break;

        case JSON_FALSE:
            print_json_false(element, indent);
            break;

        case JSON_NULL:
            print_json_null(element, indent);
            break;

        default:
            fprintf(stderr, "unrecongnized JSON type %d\n", json_typeof(element));
    }
}

static void print_json(json_t *root) { print_json_aux(root, 0); }

static json_t *load_json(char *jsonbuf)
{
    json_t *root;
    json_error_t error;

    root = json_loads(jsonbuf, 0, &error);

    if (root) {
        return root;
    } else {
        fprintf(stderr, "json error on line %d: %s\n", error.line, error.text);
        return NULL;
    }
}

static void print_json_by_jansson(char *jsonbuf)
{
    json_t *root = load_json(jsonbuf);

    if (root) {
        print_json(root);
        json_decref(root);
    }
}

static void freeRecordSchema(RecordSchema *recordSchema)
{
    if (recordSchema) {
        if (recordSchema->fields) {
            free(recordSchema->fields);
        }
        free(recordSchema);
    }
}

static void read_avro_file()
{
    avro_file_reader_t reader;
    avro_writer_t stdout_writer = avro_writer_file_fp(stdout, 1);

    avro_schema_t schema;
    avro_datum_t record;
    char *json=NULL;

    if(avro_file_reader(g_args.read_filename, &reader)) {
        fprintf(stderr, "Unable to open avro file %s: %s\n",
                g_args.read_filename, avro_strerror());
        exit(EXIT_FAILURE);
    }
    schema = avro_file_reader_get_writer_schema(reader);
    printf("*** Schema:\n");
    avro_schema_to_json(schema, stdout_writer);
    printf("\n");

    FILE *jsonfile = fopen("jsonfile.json", "w+");
    avro_writer_t jsonfile_writer;
    json_t *json_root = NULL;
    RecordSchema *recordSchema = NULL;

        printf("%d reocrdschema=%p\n", __LINE__, recordSchema);
    if (jsonfile) {
        jsonfile_writer = avro_writer_file_fp(jsonfile, 0);
        avro_schema_to_json(schema, jsonfile_writer);

        fseek(jsonfile, 0, SEEK_END);
        int size = ftell(jsonfile);

        char *jsonbuf = calloc(size + 1, 1);
        assert(jsonbuf);
        fseek(jsonfile, 0, SEEK_SET);
        fread(jsonbuf, 1, size, jsonfile);

        json_root = load_json(jsonbuf);
#ifdef DEBUG
        printf("\n%s() LN%d\n *** Schema parsed:\n", __func__, __LINE__);
        print_json(json_root);
#endif

        recordSchema = parse_json_to_recordschema(json_root);
        if (NULL == recordSchema) {
            fclose(jsonfile);
            fprintf(stderr, "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
        free(jsonbuf);
    }
    fclose(jsonfile);

    uint64_t count = 0;

    if (false == g_args.schema_only) {
        printf("\n*** Records:\n");
        avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
        avro_value_t value;
        avro_generic_value_new(value_class, &value);

        while(!avro_file_reader_read_value(reader, &value)) {
            for (int i = 0; i < recordSchema->num_fields; i++) {
                FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);
                avro_value_t field_value;
                int32_t n32;
                float f;
                int64_t n64;
                int b;
                const char *buf = NULL;
                size_t size;
                const void *bytesbuf = NULL;
                size_t bytessize;
                if (0 == avro_value_get_by_name(&value, field->name, &field_value, NULL)) {
                    if (0 == strcmp(field->type, "int")) {
                        avro_value_get_int(&field_value, &n32);
                        printf("%d | ", n32);
                    } else if (0 == strcmp(field->type, "float")) {
                        avro_value_get_float(&field_value, &f);
                        printf("%f | ", f);
                    } else if (0 == strcmp(field->type, "long")) {
                        avro_value_get_long(&field_value, &n64);
                        printf("%"PRId64" | ", n64);
                    } else if (0 == strcmp(field->type, "string")) {
                        avro_value_get_string(&field_value, &buf, &size);
                        printf("%s | ", buf);
                    } else if (0 == strcmp(field->type, "bytes")) {
                        avro_value_get_bytes(&field_value, &bytesbuf, &bytessize);
                        printf("%s | ", (char*)bytesbuf);
                    } else if (0 == strcmp(field->type, "boolean")) {
                        avro_value_get_boolean(&field_value, &b);
                        printf("%s | ", b?"true":"false");
                    }
                }
            }
            printf("\n");
        }

        avro_value_decref(&value);
        avro_value_iface_decref(value_class);
    }

    printf("\n");

    freeRecordSchema(recordSchema);
    avro_schema_decref(schema);
    avro_file_reader_close(reader);
    avro_writer_free(stdout_writer);
    avro_writer_free(jsonfile_writer);
}

static int write_record_to_file(
    avro_file_writer_t db,
    char *line,
    avro_schema_t *schema,
    RecordSchema *recordSchema)
{
    avro_value_iface_t  *wface =
        avro_generic_class_from_schema(*schema);

    avro_value_t record;
    avro_generic_value_new(wface, &record);

    char *word;

    for(int i = 0; i < recordSchema->num_fields; i++) {
        word = strsep(&line, ",");

        avro_value_t value;
        FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);
        if (avro_value_get_by_name(&record, field->name, &value, NULL) == 0) {
            if (0 == strcmp(field->type, "string")) {
                avro_value_set_string(&value, word);
            } else if (0 == strcmp(field->type, "bytes")) {
                avro_value_set_bytes(&value, (void *)word, strlen(word));
            } else if (0 == strcmp(field->type, "long")) {
                avro_value_set_long(&value, atol(word));
            } else if (0 == strcmp(field->type, "int")) {
                avro_value_set_int(&value, atoi(word));
            } else if (0 == strcmp(field->type, "boolean")) {
                avro_value_set_boolean(&value, (atoi(word))?1:0);
            } else if (0 == strcmp(field->type, "float")) {
                avro_value_set_float(&value, atof(word));
            }
        }
    }

    if (avro_file_writer_append_value(db, &record)) {
        fprintf(stderr,
                "%s() LN%d, Unable to write record to file. Message: %s",
                __func__, __LINE__,
                avro_strerror());
    }

    avro_value_decref(&record);
    avro_value_iface_decref(wface);
    return 0;
}

static RecordSchema *parse_json_to_recordschema(json_t *element)
{
    RecordSchema *recordSchema = malloc(sizeof(RecordSchema));
    assert(recordSchema);

    if (JSON_OBJECT != json_typeof(element)) {
        fprintf(stderr, "%s() LN%d, json passed is not an object\n",
                __func__, __LINE__);
        return NULL;
    }

    size_t size;
    const char *key;
    json_t *value;

    json_object_foreach(element, key, value) {
        if (0 == strcmp(key, "name")) {
            tstrncpy(recordSchema->name, json_string_value(value), RECORD_NAME_LEN-1);
        } else if (0 == strcmp(key, "fields")) {
            if (JSON_ARRAY == json_typeof(value)) {

                size_t i;
                size_t size = json_array_size(value);

#ifdef DEBUG
                printf("%s() LN%d, JSON Array of %lld element%s:\n",
                        __func__, __LINE__,
                        (long long)size, json_plural(size));
#endif

                recordSchema->num_fields = size;
                recordSchema->fields = malloc(sizeof(FieldStruct) * size);
                assert(recordSchema->fields);

                for (i = 0; i < size; i++) {
                    FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);
                    json_t *arr_element = json_array_get(value, i);
                    const char *ele_key;
                    json_t *ele_value;

                    json_object_foreach(arr_element, ele_key, ele_value) {
                        if (0 == strcmp(ele_key, "name")) {
                            tstrncpy(field->name, json_string_value(ele_value), FIELD_NAME_LEN-1);
                        } else if (0 == strcmp(ele_key, "type")) {
                            if (JSON_STRING == json_typeof(ele_value)) {
                                tstrncpy(field->type, json_string_value(ele_value), TYPE_NAME_LEN-1);
                            } else if (JSON_OBJECT == json_typeof(ele_value)) {
                                size_t obj_size;
                                const char *obj_key;
                                json_t *obj_value;

                                obj_size = json_object_size(ele_value);

                                json_object_foreach(ele_value, obj_key, obj_value) {
                                    if (0 == strcmp(obj_key, "type")) {
                                        if (JSON_STRING == json_typeof(obj_value)) {
                                            tstrncpy(field->type,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                fprintf(stderr, "%s() LN%d, fields have no array\n",
                        __func__, __LINE__);
                return NULL;
            }

            break;
        }
    }

    return recordSchema;
}

static int write_avro_file()
{
    avro_file_writer_t  file;
    avro_schema_t  writer_schema;
    avro_schema_error_t  error;
    avro_value_iface_t  *writer_iface;
    avro_value_t  writer_value;
    avro_value_t  field;

    FILE *fp = fopen(g_args.json_filename, "r");
    if (NULL == fp) {
        fprintf(stderr, "Failed to open %s\n", g_args.json_filename);
        exit(EXIT_FAILURE);
    }

    fseek(fp, 0, SEEK_END);
    int size = ftell(fp);

    char *jsonbuf = calloc(size + 1, 1);
    assert(jsonbuf);
    fseek(fp, 0, SEEK_SET);
    fread(jsonbuf, 1, size, fp);

#ifdef DEBUG
    printf("%s() LN%d\n *** json content:\n%s\n", __func__, __LINE__, jsonbuf);
#endif

    avro_schema_t schema;
    if (avro_schema_from_json_length(jsonbuf, strlen(jsonbuf), &schema)) {
        fprintf(stderr, "Unable to parse schema\n");
        fprintf(stderr, "%s() LN%d, error message: %s\n",
                __func__, __LINE__, avro_strerror());
        fclose(fp);
        free(jsonbuf);
        exit(EXIT_FAILURE);
    }

    json_t *json_root = load_json(jsonbuf);
    free(jsonbuf);

    RecordSchema *recordSchema;

    if (json_root) {
#ifdef DEBUG
        print_json(json_root);
#endif

        recordSchema = parse_json_to_recordschema(json_root);

        if (NULL == recordSchema) {
            fclose(fp);
            fprintf(stderr, "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
    } else {
        fclose(fp);
        fprintf(stderr, "json can't be parsed by jansson\n");
        exit(EXIT_FAILURE);
    }

    remove(g_args.write_filename);

    avro_file_writer_t db;

    int rval = avro_file_writer_create_with_codec
        (g_args.write_filename, schema, &db, QUICKSTOP_CODEC, 0);
    if (rval) {
        freeRecordSchema(recordSchema);
        fclose(fp);
        fprintf(stderr, "There was an error creating %s\n", g_args.write_filename);
        fprintf(stderr, "%s() LN%d, error message: %s\n",
                __func__, __LINE__,
                avro_strerror());
        exit(EXIT_FAILURE);
    }

    FILE *fd = fopen(g_args.data_filename, "r");
    if (NULL == fd) {
        freeRecordSchema(recordSchema);
        fprintf(stderr, "Failed to open %s\n", g_args.data_filename);
        fclose(fp);
        exit(EXIT_FAILURE);
    }

    ssize_t n = 0;
    ssize_t readLen = 0;
    char *line = NULL;

    fseek(fd, 0, SEEK_SET);

    while(-1 != readLen) {
        readLen = getline(&line, &n, fd);

        if (readLen != -1) {
#ifdef DEBUG
            printf("%s", line);
#endif
            write_record_to_file(db, line, &schema, recordSchema);
        }
    }

    avro_schema_decref(schema);

    free(line);
    freeRecordSchema(recordSchema);

    fclose(fd);
    fclose(fp);

    avro_file_writer_close(db);
    return 0;
}

int main(int argc, char **argv) {

    if ((argc < 2) || (false == parse_args(argc, argv, &g_args))) {
        printHelp();
        exit(0);
    }

    if (g_args.read_file || g_args.schema_only) {
        read_avro_file();
    } else if (g_args.write_file) {
        if (0 == write_avro_file()) {
            printf("\nSuccess!\n");
        } else {
            printf("\nFailed!\n");
        }
    }
    exit(EXIT_SUCCESS);
}


