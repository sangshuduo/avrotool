#include <stdio.h>
#include <stdbool.h>
#include <limits.h>
#include <avro.h>

typedef struct SArguments_S {
    bool read_file;
    char *read_filename;
    unsigned int count;
    bool schema_only;
    bool write_file;
    char *write_filename;
    char *json_filename;
} SArguments;

SArguments g_args = {
    false,          // read_file
    "",             // read_filename
    UINT_MAX,       // count
    false,          // schema_only
    false,          // write_file
    "",             // write_filename
    "",             // json_filename
};

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
    printf("%s%s%s%s\n", indent, "--help\t", indent,
            "Print command line arguments list info.");

    printf("\n");
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
            arguments->count = atoi(argv[i+1]);
        } else if (strcmp(argv[i], "-w") == 0) {
            arguments->write_file = true;
            arguments->write_file = argv[i+1];
        } else if (strcmp(argv[i], "-m") == 0) {
            arguments->json_filename = argv[i+1];
        } else if (strcmp(argv[i], "--help") == 0) {
            printHelp();
            exit(0);
        } else {
            has_flags = false;
        }
    }

    return has_flags;
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
    avro_schema_decref(schema);

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

static void write_avro_file()
{

}

int main(int argc, char **argv) {

    if ((argc < 2) || (false == parse_args(argc, argv, &g_args))) {
        printHelp();
        exit(0);
    }

    if (g_args.read_file) {
        read_avro_file();
    }

    if (g_args.write_file) {
        write_avro_file();
    }
    exit(EXIT_SUCCESS);
}


