#include <stdio.h>
#include <limits.h>
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
    bool nullable;
    bool is_array;
    char array_type[TYPE_NAME_LEN];
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
    bool debug_output;
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
    false,          // debug_output
};


#define debugPrint(fmt, ...) \
    do { if (g_args.debug_output) \
      fprintf(stderr, "DEBG: "fmt, __VA_ARGS__); } while(0)

#define verbosePrint(fmt, ...) \
    do { if (g_args.verbose_print) \
        fprintf(stderr, "VERB: "fmt, __VA_ARGS__); } while(0)

#define performancePrint(fmt, ...) \
    do { if (g_args.performance_print) \
        fprintf(stderr, "PERF: "fmt, __VA_ARGS__); } while(0)

#define warnPrint(fmt, ...) \
    do { fprintf(stderr, "\033[33m"); \
        fprintf(stderr, "WARN: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define errorPrint(fmt, ...) \
    do { fprintf(stderr, "\033[31m"); \
        fprintf(stderr, "ERROR: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

#define okPrint(fmt, ...) \
    do { fprintf(stderr, "\033[32m"); \
        fprintf(stderr, "OK: "fmt, __VA_ARGS__); \
        fprintf(stderr, "\033[0m"); } while(0)

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
    printf("%s%s%s%s\n", indent, "-g\t", indent,
            "print debug info.");
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
        } else if (strcmp(argv[i], "-g") == 0) {
            arguments->debug_output = true;
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
            errorPrint("unrecongnized JSON type %d\n", json_typeof(element));
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
        errorPrint("json error on line %d: %s\n", error.line, error.text);
        return NULL;
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
        errorPrint("Unable to open avro file %s: %s\n",
                g_args.read_filename, avro_strerror());
        exit(EXIT_FAILURE);
    }
    schema = avro_file_reader_get_writer_schema(reader);
    printf("=== Schema:\n");
    avro_schema_to_json(schema, stdout_writer);
    printf("\n");

    FILE *jsonfile = fopen("jsonfile.json", "w+");
    avro_writer_t jsonfile_writer;
    json_t *json_root = NULL;
    RecordSchema *recordSchema = NULL;

    debugPrint("%s() LN%d reocrdschema=%p\n",
            __func__, __LINE__, recordSchema);
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
        if (g_args.debug_output) {
            printf("\n%s() LN%d\n === Schema parsed:\n", __func__, __LINE__);
            print_json(json_root);
        }

        recordSchema = parse_json_to_recordschema(json_root);
        if (NULL == recordSchema) {
            fclose(jsonfile);
            errorPrint("%s", "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
        free(jsonbuf);
    }
    fclose(jsonfile);

    uint64_t count = 0;

    if (false == g_args.schema_only) {
        printf("\n=== Records:\n");
        avro_value_iface_t *value_class = avro_generic_class_from_schema(schema);
        avro_value_t value;
        avro_generic_value_new(value_class, &value);

        while(!avro_file_reader_read_value(reader, &value)) {
            for (int i = 0; i < recordSchema->num_fields; i++) {
                FieldStruct *field = (FieldStruct *)(recordSchema->fields + sizeof(FieldStruct) * i);
                avro_value_t field_value;
                int32_t n32;
                float f;
                double dbl;
                int64_t n64;
                int b;
                const char *buf = NULL;
                size_t size;
                const void *bytesbuf = NULL;
                size_t bytessize;
                if (0 == avro_value_get_by_name(&value, field->name, &field_value, NULL)) {
                    if (0 == strcmp(field->type, "int")) {
                        avro_value_get_int(&field_value, &n32);
                        if (n32 == -2147483648) {
                            printf("null |\t");
                        } else {
                            printf("%d |\t", n32);
                        }
                    } else if (0 == strcmp(field->type, "float")) {
                        avro_value_get_float(&field_value, &f);
                        printf("%f |\t", f);
                    } else if (0 == strcmp(field->type, "double")) {
                        avro_value_get_double(&field_value, &dbl);
                        printf("%f |\t", dbl);
                    } else if (0 == strcmp(field->type, "long")) {
                        avro_value_get_long(&field_value, &n64);
                        printf("%"PRId64" |\t", n64);
                    } else if (0 == strcmp(field->type, "string")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            avro_value_get_string(&branch, &buf, &size);
                        } else {
                            avro_value_get_string(&field_value, &buf, &size);
                        }
                        printf("%s |\t", buf);
                    } else if (0 == strcmp(field->type, "bytes")) {
                        if (field->nullable) {
                            avro_value_t branch;
                            avro_value_get_current_branch(&field_value,
                                    &branch);
                            avro_value_get_bytes(&branch, &bytesbuf,
                                    &bytessize);
                        } else {
                            avro_value_get_bytes(&field_value, &bytesbuf,
                                    &bytessize);
                        }
                        printf("%s |\t", (char*)bytesbuf);
                    } else if (0 == strcmp(field->type, "boolean")) {
                        if (field->nullable) {
                            avro_value_t bool_branch;
                            avro_value_get_current_branch(&field_value,
                                    &bool_branch);
                            if (0 == avro_value_get_null(&bool_branch)) {
                                printf("%s |\t", "null");
                            } else {
                                avro_value_get_boolean(&bool_branch, &b);
                                printf("%s |\t", b?"true":"false");
                            }
                        } else {
                            avro_value_get_boolean(&field_value, &b);
                            printf("%s |\t", b?"true":"false");
                        }
                    } else if (0 == strcmp(field->type, "array")) {
                        size_t array_size;
                        avro_value_get_size(&field_value, &array_size);

                        debugPrint("array_size is %d\n", (int) array_size);
                        if (0 == strcmp(field->array_type, "int")) {
                            uint32_t array_u32 = 0;
                            for (size_t item = 0; item < array_size; item ++) {
                                avro_value_t item_value;
                                avro_value_get_by_index(&field_value, item,
                                        &item_value, NULL);
                                avro_value_get_int(&item_value, &n32);
                                array_u32 += n32;
                            }
                            printf("%u |\t", array_u32);
                        } else if (0 == strcmp(field->array_type, "long")) {
                            uint64_t array_u64 = 0;
                            for (size_t item = 0; item < array_size; item ++) {
                                avro_value_t item_value;
                                avro_value_get_by_index(&field_value, item,
                                        &item_value, NULL);
                                avro_value_get_long(&item_value, &n64);
                                array_u64 += n64;
                            }
                            printf("%"PRIu64" |\t", array_u64);
                        } else {
                            errorPrint("%s is not supported!\n",
                                    field->array_type);
                        }
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
                avro_value_t branch;
                if ((field->nullable) && (0 == strcmp(word, "null"))) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_string(&branch, word);
                }
            } else if (0 == strcmp(field->type, "bytes")) {
                avro_value_t branch;
                if ((field->nullable) && (0 == strcmp(word, "null"))) {
                    avro_value_set_branch(&value, 0, &branch);
                    avro_value_set_null(&branch);
                } else {
                    avro_value_set_branch(&value, 1, &branch);
                    avro_value_set_bytes(&branch, (void *)word, strlen(word));
                }
            } else if (0 == strcmp(field->type, "long")) {
                avro_value_set_long(&value, atol(word));
            } else if (0 == strcmp(field->type, "int")) {
                avro_value_set_int(&value, atoi(word));
            } else if (0 == strcmp(field->type, "boolean")) {
                avro_value_t bool_branch;
                if (0 == strcmp(word, "null")) {
                    avro_value_set_branch(&value, 0, &bool_branch);
                    avro_value_set_null(&bool_branch);
                } else {
                    avro_value_set_branch(&value, 1, &bool_branch);
                    avro_value_set_boolean(&bool_branch, (atoi(word))?1:0);
                }
            } else if (0 == strcmp(field->type, "float")) {
                avro_value_set_float(&value, atof(word));
            } else if (0 == strcmp(field->type, "array")) {
                if (0 == strcmp(field->array_type, "int")) {
                    avro_value_t intv1, intv2;
                    unsigned long ultemp;
                    char *eptr;
                    ultemp = strtoul(word, &eptr, 10);
                    avro_value_append(&value, &intv1, NULL);
                    avro_value_set_int(&intv1, (int32_t)(ultemp - INT_MAX));
                    avro_value_append(&value, &intv2, NULL);
                    avro_value_set_int(&intv2, INT_MAX);
                } else if (0 == strcmp(field->array_type, "long")) {
                    avro_value_t longv1, longv2;
                    unsigned long long int ulltemp;
                    char *eptr;
                    ulltemp = strtoull(word, &eptr, 10);
                    if ( errno || (!ulltemp && word == NULL) ) {
                        fflush(stdout); // Don't cross the streams!
                        perror(word);
//                        exit(EXIT_FAILURE);
                    }
                    avro_value_append(&value, &longv1, NULL);
                    avro_value_set_long(&longv1, (int64_t)(ulltemp - LONG_MAX));
                    avro_value_append(&value, &longv2, NULL);
                    avro_value_set_long(&longv2, LONG_MAX);
                }
            }
        }
    }

    if (avro_file_writer_append_value(db, &record)) {
        errorPrint(
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
    RecordSchema *recordSchema = calloc(1, sizeof(RecordSchema));
    assert(recordSchema);

    if (JSON_OBJECT != json_typeof(element)) {
        errorPrint("%s() LN%d, json passed is not an object\n",
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

                if (g_args.debug_output) {
                    printf("%s() LN%d, JSON Array of %lld element%s:\n",
                            __func__, __LINE__,
                            (long long)size, json_plural(size));
                }

                recordSchema->num_fields = size;
                recordSchema->fields = calloc(1, sizeof(FieldStruct) * size);
                assert(recordSchema->fields);

                for (i = 0; i < size; i++) {
                    FieldStruct *field = (FieldStruct *)
                        (recordSchema->fields + sizeof(FieldStruct) * i);
                    json_t *arr_element = json_array_get(value, i);
                    const char *ele_key;
                    json_t *ele_value;

                    json_object_foreach(arr_element, ele_key, ele_value) {
                        if (0 == strcmp(ele_key, "name")) {
                            tstrncpy(field->name,
                                    json_string_value(ele_value),
                                    FIELD_NAME_LEN-1);
                        } else if (0 == strcmp(ele_key, "type")) {
                            int ele_type = json_typeof(ele_value);

                            if (JSON_STRING == ele_type) {
                                tstrncpy(field->type,
                                        json_string_value(ele_value),
                                        TYPE_NAME_LEN-1);
                            } else if (JSON_ARRAY == ele_type) {
                                size_t ele_size = json_array_size(ele_value);

                                for(size_t ele_i = 0; ele_i < ele_size;
                                        ele_i ++) {
                                    json_t *arr_type_ele =
                                        json_array_get(ele_value, ele_i);

                                    if (JSON_STRING == json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_str =
                                            json_string_value(arr_type_ele);

                                        if(0 == strcmp(arr_type_ele_str,
                                                            "null")) {
                                            field->nullable = true;
                                        } else {
                                            tstrncpy(field->type,
                                                    arr_type_ele_str,
                                                    TYPE_NAME_LEN-1);
                                        }
                                    } else if (JSON_OBJECT ==
                                            json_typeof(arr_type_ele)) {
                                        const char *arr_type_ele_key;
                                        json_t *arr_type_ele_value;

                                        json_object_foreach(arr_type_ele,
                                                arr_type_ele_key,
                                                arr_type_ele_value) {
                                            if (JSON_STRING ==
                                                    json_typeof(arr_type_ele_value)) {
                                                const char *arr_type_ele_value_str =
                                                    json_string_value(arr_type_ele_value);
                                                if(0 == strcmp(arr_type_ele_value_str,
                                                            "null")) {
                                                    field->nullable = true;
                                                } else {
                                                    tstrncpy(field->type,
                                                            arr_type_ele_value_str,
                                                            TYPE_NAME_LEN-1);
                                                }
                                            }
                                        }
                                    } else {
                                        errorPrint("%s", "Error: not supported!\n");
                                    }
                                }
                            } else if (JSON_OBJECT == ele_type) {
                                const char *obj_key;
                                json_t *obj_value;

                                json_object_foreach(ele_value, obj_key, obj_value) {
                                    if (0 == strcmp(obj_key, "type")) {
                                        int obj_value_type = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_type) {
                                            tstrncpy(field->type,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                            if (0 == strcmp(field->type, "array")) {
                                                field->is_array = true;
                                            }
                                        } else if (JSON_OBJECT == obj_value_type) {
                                            const char *field_key;
                                            json_t *field_value;

                                            json_object_foreach(obj_value, field_key, field_value) {
                                                if (JSON_STRING == json_typeof(field_value)) {
                                                    tstrncpy(field->type,
                                                            json_string_value(field_value),
                                                            TYPE_NAME_LEN-1);
                                                } else {
                                                    field->nullable = true;
                                                }
                                            }
                                        }
                                    } else if (0 == strcmp(obj_key, "items")) {
                                        int obj_value_items = json_typeof(obj_value);
                                        if (JSON_STRING == obj_value_items) {
                                            field->is_array = true;
                                            tstrncpy(field->array_type,
                                                    json_string_value(obj_value), TYPE_NAME_LEN-1);
                                        } else if (JSON_OBJECT == obj_value_items) {
                                            const char *item_key;
                                            json_t *item_value;

                                            json_object_foreach(obj_value, item_key, item_value) {
                                                if (JSON_STRING == json_typeof(item_value)) {
                                                    tstrncpy(field->array_type,
                                                            json_string_value(item_value),
                                                            TYPE_NAME_LEN-1);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                errorPrint("%s() LN%d, fields have no array\n",
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
        errorPrint("Failed to open %s\n", g_args.json_filename);
        exit(EXIT_FAILURE);
    }

    fseek(fp, 0, SEEK_END);
    int size = ftell(fp);

    char *jsonbuf = calloc(size + 1, 1);
    assert(jsonbuf);
    fseek(fp, 0, SEEK_SET);
    fread(jsonbuf, 1, size, fp);

    if (g_args.debug_output) {
        printf("%s() LN%d\n === json content:\n%s\n", __func__, __LINE__, jsonbuf);
    }

    avro_schema_t schema;
    if (avro_schema_from_json_length(jsonbuf, strlen(jsonbuf), &schema)) {
        errorPrint("%s", "Unable to parse schema\n");
        errorPrint("%s() LN%d, error message: %s\n",
                __func__, __LINE__, avro_strerror());
        fclose(fp);
        free(jsonbuf);
        exit(EXIT_FAILURE);
    }

    if (g_args.debug_output) {
        avro_writer_t stdout_writer = avro_writer_file_fp(stdout, 1);
        printf("=== convert Schema back to json:\n");
        avro_schema_to_json(schema, stdout_writer);
        printf("\n");
        avro_writer_free(stdout_writer);
    }

    json_t *json_root = load_json(jsonbuf);
    free(jsonbuf);

    RecordSchema *recordSchema;

    if (json_root) {
        if (g_args.debug_output) {
            print_json(json_root);
        }

        recordSchema = parse_json_to_recordschema(json_root);

        if (NULL == recordSchema) {
            fclose(fp);
            errorPrint("%s", "Failed to parse json to recordschema\n");
            exit(EXIT_FAILURE);
        }

        json_decref(json_root);
    } else {
        fclose(fp);
        errorPrint("%s", "json can't be parsed by jansson\n");
        exit(EXIT_FAILURE);
    }

    remove(g_args.write_filename);

    avro_file_writer_t db;

    int rval = avro_file_writer_create_with_codec
        (g_args.write_filename, schema, &db, QUICKSTOP_CODEC, 0);
    if (rval) {
        freeRecordSchema(recordSchema);
        fclose(fp);
        errorPrint("There was an error creating %s\n", g_args.write_filename);
        errorPrint("%s() LN%d, error message: %s\n",
                __func__, __LINE__,
                avro_strerror());
        exit(EXIT_FAILURE);
    }

    FILE *fd = fopen(g_args.data_filename, "r");
    if (NULL == fd) {
        freeRecordSchema(recordSchema);
        errorPrint("Failed to open %s\n", g_args.data_filename);
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
            if (g_args.debug_output) {
                printf("%s", line);
            }
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
            okPrint("%s", "Success!\n");
        } else {
            errorPrint("%s", "Failed!\n");
        }
    }
    fflush(stdout);
    exit(EXIT_SUCCESS);
}


