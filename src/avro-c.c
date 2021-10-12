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

static void print_json_aux(json_t *element, int indent);

static void printHelp()
{
    char indent[10] = "        ";

    printf("\n");

    printf("%s\n\n", "avro-c usage:");
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

static void read_avro_file()
{
    avro_file_reader_t reader;
    avro_writer_t stdout_writer = avro_writer_file_fp(stdout, 0);

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
    if (jsonfile) {
        avro_writer_t jsonfile_writer = avro_writer_file_fp(jsonfile, 0);
        avro_schema_to_json(schema, jsonfile_writer);

        fseek(jsonfile, 0, SEEK_END);
        int size = ftell(jsonfile);

        char *jsonbuf = calloc(size, 1);
        assert(jsonbuf);
        fseek(jsonfile, 0, SEEK_SET);
        fread(jsonbuf, 1, size, jsonfile);

#ifdef DEBUG
        printf("\n*** Schema parsed:\n");
        print_json_by_jansson(jsonbuf);
#endif
        free(jsonbuf);
    }
    fclose(jsonfile);

    avro_schema_decref(schema);
    printf("\n");

    unsigned int count = 0;
    if (false == g_args.schema_only) {
        printf("\n*** Records:\n");
        while(!avro_file_reader_read(reader, NULL, &record)
                && (count < g_args.count)) {
            avro_datum_to_json(record, 0, &json);
            puts(json);
            free(json);
            avro_datum_decref(record);
            count++;
        }
    }

    avro_file_reader_close(reader);
    printf("\n");
}

static int write_record_to_file(
    avro_file_writer_t *db,
    char *line,
    avro_schema_t *schema)
{
    avro_value_iface_t  *iface =
        avro_generic_class_from_schema(*schema);

    avro_value_t record;
    avro_generic_value_new(iface, &record);

    return 0;
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

    char *jsonbuf = calloc(size, 1);
    assert(jsonbuf);
    fseek(fp, 0, SEEK_SET);
    fread(jsonbuf, 1, size, fp);

#ifdef DEBUG
    printf("json content:\n%s\n", jsonbuf);
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

    remove(g_args.write_filename);

    avro_file_writer_t db;

    int rval = avro_file_writer_create_with_codec
        (g_args.write_filename, schema, &db, QUICKSTOP_CODEC, 0);
    if (rval) {
        fclose(fp);
        fprintf(stderr, "There was an error creating %s\n", g_args.write_filename);
        fprintf(stderr, "%s() LN%d, error message: %s\n",
                __func__, __LINE__,
                avro_strerror());
        exit(EXIT_FAILURE);
    }

    FILE *fd = fopen(g_args.data_filename, "r");
    if (NULL == fd) {
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
            write_record_to_file(&db, line, &schema);
        }

    }

    free(line);

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
        write_avro_file();
    }
    exit(EXIT_SUCCESS);
}


