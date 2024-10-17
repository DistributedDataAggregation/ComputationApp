#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>
#include "threads_functionality/thread_computation/thread_computation.h"
#include "threads_functionality/thread_argument.h"
#include "file_handling/file_handling.h"
#include "consts/POSITIONS.h"
#include "threads_functionality/results.h"
#include <parquet-glib/arrow-file-reader.h>

#define NUM_THREADS 4

char* get_data_file_name_from_command_line_arguments(int argc, char** argv);

void print_schema_fields(GArrowSchema *schema) {
    gint n_fields = garrow_schema_n_fields(schema);

    for (gint i = 0; i < n_fields; i++) {
        GArrowField *field = garrow_schema_get_field(schema, i);
        const gchar *field_name = garrow_field_get_name(field);
        GArrowDataType* field_type = garrow_field_get_data_type(field);

        // Convert GArrowType to a string for easier reading
        const gchar *field_type_name = garrow_data_type_to_string(field_type);

        g_print("Field Name: %s, Field Type: %s\n", field_name, field_type_name);
    }
}

int main(int argc, char** argv)
{
    char* data_file = get_data_file_name_from_command_line_arguments(argc, argv);
    if(data_file != NULL)
        printf("Provided file name: %s\n", data_file);

    struct Results results[POSITIONS];

    for(int i = 0; i < POSITIONS; i++) {
        results[i].count = 0;
        results[i].summed_age = 0;
        results[i].summed_salary = 0.0f;
    }

    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(data_file, NULL);
    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, NULL);

    print_schema_fields(schema);
    GArrowChunkedArray* chunked_array = gparquet_arrow_file_reader_read_column_data(reader, 3, NULL);


    g_object_unref(reader);
    g_object_unref(schema);
    g_object_unref(chunked_array);


     pthread_t threads[NUM_THREADS];

     struct ThreadArgs thread_args[2];
     for(int i = 0; i < NUM_THREADS; i++) {
         thread_args[i].filename = data_file;
         thread_args[i].threads_count = NUM_THREADS;
         thread_args[i].thread_index = i;
         pthread_create(&threads[i], NULL, &compute_on_thread, &thread_args[i]);
     }

     int total_count = 0;

     for(int i = 0; i < NUM_THREADS; i++) {
         void* retval;
         pthread_join(threads[i], &retval);

         int *count_from_thread = (int *) retval;
         printf("Thread computed %d rows\n", *count_from_thread);
         free(count_from_thread);
         printf("Thread %d returned\n", i);
    }

    printf("Total number of records: %d\n", total_count);
    return EXIT_SUCCESS;
}

char* get_data_file_name_from_command_line_arguments(int argc, char** argv)
{
    char* data_file= NULL;
    int c;
    while((c = getopt(argc, argv, "f:")) != -1) {
        switch(c) {
            case 'f':
                data_file = optarg;
            break;
            case '?':
                fprintf(stderr, "Usage: %s -f <filename>\n", argv[0]);
            break;
            default:
                fprintf(stderr, "Usage: %s -f <filename>\n", argv[0]);
        }
    }

    return data_file;
}


