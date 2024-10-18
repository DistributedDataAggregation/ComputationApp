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

#define NUM_THREADS 10

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

     struct ThreadArgs thread_args[NUM_THREADS];
     for(int i = 0; i < NUM_THREADS; i++) {
         thread_args[i].filename = data_file;
         thread_args[i].threads_count = NUM_THREADS;
         thread_args[i].thread_index = i;
         pthread_create(&threads[i], NULL, &compute_on_thread, &thread_args[i]);
     }

     int total_count = 0;

    void* retval;
    pthread_join(threads[0], &retval);
    struct Results *partial_results = (struct Results *) retval;
    for(int position_index=0 ; position_index<POSITIONS; position_index++) {
        results[position_index].position = (char*) malloc(strlen(partial_results[position_index].position));
        strcpy(results[position_index].position, partial_results[position_index].position);
        results[position_index].count = partial_results[position_index].count;
        results[position_index].summed_age = partial_results[position_index].summed_age;
        results[position_index].summed_salary = partial_results[position_index].summed_salary;

        total_count += partial_results[position_index].count;
        printf("Thread %d returned\n", 0);
        free(partial_results[position_index].position);
    }

    free(partial_results);

     for(int i = 1; i < NUM_THREADS; i++) {
         pthread_join(threads[i], &retval);
         struct Results *partial_results = (struct Results *) retval;

         for(int position_index=0 ; position_index<POSITIONS; position_index++) {

             for(int j=0; j<POSITIONS;j++) {
                 if(strcmp(partial_results[position_index].position, results[j].position) == 0) {
                     results[j].count = partial_results[position_index].count;
                     results[j].summed_age = partial_results[position_index].summed_age;
                     results[j].summed_salary = partial_results[position_index].summed_salary;
                 }
             }
             //printf("There are %ld %s's\n", partial_results[position_index].count, partial_results[position_index].position);
             // printf("The summed salary is %f\n", partial_results[i].summed_salary);
             // printf("Position %s: average age=%2f, average salary=%2f\n",
             //      partial_results[position_index].position,
             //      (partial_results[position_index].summed_age/(float)partial_results[position_index].count),
             //      (partial_results[position_index].summed_salary/(float)partial_results[position_index].count));

             total_count += partial_results[position_index].count;
             free(partial_results[position_index].position);
         }

          free(partial_results);

         printf("Thread %d returned\n", i);
    }

    for(int i=0;i<POSITIONS;i++) {
        printf("There are %ld %s with average age of %ld and average salary of %ld\n",
            results[i].count, results[i].position, (results[i].summed_age/results[i].count), (results[i].summed_salary/results[i].count));
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


