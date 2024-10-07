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

void* compute_on_thread(void* arg);

int main(int argc, char** argv) 
{
    char* data_file = get_data_file_name_from_command_line_arguments(argc, argv);
    if(data_file != NULL)
        printf("Provided file name: %s\n", data_file);

    //struct Results results[POSITIONS];
    //int found_positons = 0;

    pthread_t threads[NUM_THREADS];

    struct ThreadArgs thread_args[2];
    for(int i = 0; i < NUM_THREADS; i++) {
        thread_args[i].filename = data_file;
        thread_args[i].threads_count = NUM_THREADS;
        thread_args[i].thread_index = i;
        pthread_create(&threads[i], NULL, &test_thread, &thread_args[i]);
    }

    int total_count = 0;

    for(int i = 0; i < NUM_THREADS; i++) {
        void* retval;
        pthread_join(threads[i], &retval);

        //struct Results *partial_results = (struct Results *) retval;

        // for(int i=0;i<POSITIONS;i++) {
        //     printf("Position %s: average age=%2f, average salary=%2f\n",
        //         partial_results[i].position,
        //         (partial_results[i].summed_age/(float)partial_results[i].count),
        //         (partial_results[i].summed_salary/(float)partial_results[i].count));
        //
        //     total_count += partial_results[i].count;
        //
        //     free(partial_results[i].position);
        // }
        // free(partial_results);
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

void* compute_on_thread(void* arg)
{
    struct Results* results = malloc(sizeof(struct Results)*POSITIONS);
    int found_positions = 0;

    struct ThreadArgs* thread_args = (struct ThreadArgs*)arg;

    FILE* file = open_file_for_reading(thread_args->filename);
    int file_descriptor = fileno(file);

    struct stat file_stat;
    fstat(file_descriptor, &file_stat);

    long bytes_per_thread = (file_stat.st_size/thread_args->threads_count);
    long start_for_thread = bytes_per_thread*thread_args->thread_index;
    long end_for_thread = start_for_thread + bytes_per_thread;
    char line[1024];

    // skip to the start of this threads part
    if (fseek(file, start_for_thread, SEEK_CUR) != 0) {
        perror("Failed to seek");
        return NULL; // Handle the error
    }

    // skip the first line, we expect the previous thread to read it,
    // in case of thread_index = 0, first line is column names
    fgets(line, sizeof(line), file);

    int iterations =0;
    while (fgets(line, sizeof(line), file)) {
        iterations++;

        // Remove the newline character from the end if present
        int current_position_index = -1;
        line[strcspn(line, "\n")] = '\0';
        // Parse the CSV line

        char *token;
        int column = 0;

        // Tokenize the line by commas
        token = strtok(line, ",");
        while (token != NULL && column < 5) {
            //printf("Column %d: %s\n", column, token);

            if(column == 2) {
                char *current_position = token;
                for(int i=0;i<found_positions;i++) {
                    if(strcmp(results[i].position, current_position) == 0) {
                        current_position_index = i;
                    }
                }

                if(current_position_index == -1) {
                    results[found_positions].position = (char*)malloc((strlen(token) + 1)*sizeof(char));
                    results[found_positions].summed_age = 0;
                    results[found_positions].summed_salary = 0;
                    results[found_positions].count = 0;

                    strcpy(results[found_positions].position, current_position);
                    current_position_index=found_positions++;
                }

                results[current_position_index].count++;
            } else if (column == 3) {
                //const long age = strtol(token, NULL, 10);
                //results[current_position_index].summed_age += age;
            } else if (column == 4) {
                //const float salary = atof(token);
                //results[current_position_index].summed_salary +=salary;
            }
            token = strtok(NULL, ",");
            column++;
        }
    }
    fclose(file);

    return (void*)results;
}

