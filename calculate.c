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

#define NUM_THREADS 2

char* get_data_file_name_from_command_line_arguments(int argc, char** argv);


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

        struct Results *partial_results = (struct Results *) retval;




         for(int i=0;i<POSITIONS;i++) {
             printf("Position %s: average age=%2f, average salary=%2f\n",
                 partial_results[i].position,
                 (partial_results[i].summed_age/(float)partial_results[i].count),
                 (partial_results[i].summed_salary/(float)partial_results[i].count));

             total_count += partial_results[i].count;

             free(partial_results[i].position);
         }

         free(partial_results);

        printf("Thread %d returned\n", i);
    }

    printf("Total number of records: %d\n", total_count);

    // for(int i=0;i<POSITIONS;i++) {
    //     free(results[i].position);
    // }

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


