#include <stdio.h>
#include <avro.h>

typedef struct SArguments_S {
    char *filename;
} SArguments;

SArguments g_args = {
    ""          // filename
};

static void printHelp()
{
    char indent[10] = "        ";

    printf("\n");

    printf("%s\n\n", "avro-c usage:");
    printf("%s%s%s%s\n", indent, "-r\t", indent,
            "<avro filename>.");
    printf("%s%s%s%s\n", indent, "--help\t", indent,
            "Print command line arguments list info.");

    printf("\n");
}

static void parse_args(int argc, char *argv[], SArguments *arguments)
{
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-r") == 0) {
            arguments->filename = argv[++i];
        } else if (strcmp(argv[i], "--help") == 0) {
            printHelp();
            exit(0);
        }
    }
}


int main(int argc, char **argv) {

    if (argc < 2) {
        printHelp();
        exit(0);
    }

    parse_args(argc, argv, &g_args);

    avro_file_reader_t reader;
    avro_writer_t stdout_writer = avro_writer_file_fp(stdout, 0);
    avro_schema_t schema;
    avro_datum_t record;
    char *json=NULL;

    if(avro_file_reader(g_args.filename, &reader)) {
        fprintf(stderr, "Unable to open avro file %s: %s\n", argv[1], avro_strerror());
        exit(EXIT_FAILURE);
    }
    schema = avro_file_reader_get_writer_schema(reader);
    printf("*** Schema:\n");
    avro_schema_to_json(schema, stdout_writer);
    printf("\n*** Records:\n");
    while(!avro_file_reader_read(reader, NULL, &record)) {
        avro_datum_to_json(record, 0, &json);
        puts(json);
        free(json);
        avro_datum_decref(record);
    }

    avro_schema_decref(schema);
    avro_file_reader_close(reader);
    printf("\n");
    exit(EXIT_SUCCESS);
}
