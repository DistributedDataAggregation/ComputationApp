//
// Created by karol on 10/7/24.
//

#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "thread_computation.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "../thread_argument.h"
#include "../../file_handling/file_handling.h"
#include "../../consts/POSITIONS.h"
#include "../results.h"
#include <parquet-glib/arrow-file-reader.h>

#define LINE_SIZE 1024
#define NUM_OF_COLUMNS 3
#define NEW_LINE "\n"
#define NOT_FOUND_POSITION -1

typedef struct {
    const char *title;
    float salary;
    int age;
} PositionInfo;

PositionInfo positions[] = {
    {"Manager", 80000, 45},
    {"Developer", 10000, 28},
    {"Designer", 60000, 32},
    {"Analyst", 70000, 30},
    {"Tester", 50000, 27},
    {"Sales", 75000, 35},
    {"HR", 65000, 40},
    {"Support", 40000, 26},
    {"Marketing", 72000, 33},
    {"Consultant", 90000, 38}
};

void skip_line(FILE* fp);
int get_position_index(const struct Results* results, const char* position, const int found_positions);
int setup_new_position_data(struct Results* results, const char* position, int* found_positions, int position_index);

void* compute_on_thread(void* arg)
{
    int found_positions = 0;
    struct Results* results = malloc(sizeof(struct Results)*POSITIONS);
    struct ThreadArgs* thread_args = (struct ThreadArgs*)arg;
    int thread_index = thread_args->thread_index;

    printf("Thread number %d started\n", thread_index);
    printf("Filename %s\n", thread_args->filename);

    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(thread_args->filename, &error);
    if(reader == NULL) {
        if (error != NULL) {
            g_print("Error occurred: %s\n", error->message);
            g_error_free(error);  // Free the GError after use
        } else {
            g_print("Unknown error occurred.\n");
        }
        printf("Thread %d is quiting because of failed file opening attempt\n", thread_index);
        return NULL;
    }

    gint row_groups_count = gparquet_arrow_file_reader_get_n_row_groups(reader);

    printf("There are %d chunks\n", row_groups_count);

    int row_groups_per_thread = (row_groups_count + (thread_args->threads_count-1)) / thread_args->threads_count;
    int start_for_thread = row_groups_per_thread*thread_index;
    int end_for_thread = start_for_thread + row_groups_per_thread;

    int* total_count_for_thread = (int*)(malloc(sizeof(int)));
    *total_count_for_thread = 0;

    gint columns_indices[NUM_OF_COLUMNS] = {2,3,4};
    for(int i=start_for_thread; i< row_groups_count && i < end_for_thread; i++) {
        GArrowTable* table = gparquet_arrow_file_reader_read_row_group (reader, i, columns_indices, NUM_OF_COLUMNS, NULL);

        GArrowChunkedArray* position_chunk_array = garrow_table_get_column_data (table,0);
        GArrowChunkedArray* age_chunk_array = garrow_table_get_column_data (table,1);
        GArrowChunkedArray* salary_chunk_array = garrow_table_get_column_data (table,2);

        gint chunks_count = garrow_chunked_array_get_n_chunks(position_chunk_array);

        for(int j=0; j< chunks_count; j++) {
            GArrowArray* positions_array = garrow_chunked_array_get_chunk(position_chunk_array, j);
            GArrowStringArray* string_array = GARROW_STRING_ARRAY(positions_array);

            GArrowArray* ages_array = garrow_chunked_array_get_chunk(age_chunk_array, j);
            GArrowInt64Array* ages_int_array = GARROW_INT64_ARRAY(ages_array);

            GArrowArray* salaries_array = garrow_chunked_array_get_chunk(salary_chunk_array, j);
            GArrowInt64Array* salaries_int_array = GARROW_INT64_ARRAY(salaries_array);

            gint count = garrow_array_count(positions_array, NULL, NULL);
            gchar* data = garrow_array_to_string(positions_array, NULL);

            char* state;
            int test_counter = 0;
            for(int k = 0; k < count; k++) {
                gchar* position = garrow_string_array_get_string(string_array, k);
                int position_index = get_position_index(results, position, found_positions);
                if(position_index == NOT_FOUND_POSITION) {
                    position_index = setup_new_position_data(results, position, &found_positions, position_index);
                }

                test_counter++;
                results[position_index].count++;
                results[position_index].summed_age += garrow_int64_array_get_value(ages_int_array, k);
                results[position_index].summed_salary += garrow_int64_array_get_value(salaries_int_array, k);
            }

            (*total_count_for_thread) += count;
            g_free(data);
            g_object_unref(positions_array);
        }
        g_object_unref(table);
    }

    for(int i=0;i<POSITIONS;i++) {
        printf("There are %ld %ss'\n", results[i].count, results[i].position);
    }

    g_object_unref(reader);

    printf("Thread %d has read %d lines\n", thread_index, *total_count_for_thread);
    return results;
}

int parse_chunked_row_group(char* positions_chunk, char* ages_chunk, char* salaries_chunk)
{
    int read_lines = 0;


    // printf("Thread %d has opened the file for reading\n", thread_index);
    //
    // char line[LINE_SIZE];
    // char line_copy[LINE_SIZE];
    //
    // int read_lines = 0;
    // int allocations = 0;
    //
    // int found_positions = 0;
    //
    // fgets(line, sizeof(line), fp);
    //
    // skip_line(fp);
    // while(ftell(fp) <= end_for_thread && fgets(line, sizeof(line), fp)) {
    //     read_lines++;
    //
    //     line[strcspn(line, "\n")] = '\0';
    //     strcpy(line_copy, line);
    //
    //     int current_position_index = -1;
    //     char* token, *state;
    //     int column = 0;
    //
    //     token = strtok_r(line, ",", &state);
    //     PositionInfo current_position_info;
    //     while(token != NULL && column < 5) {
    //
    //         if(column == 2) {
    //             char* current_position = token;
    //
    //             for(int i=0; i < found_positions; i++) {
    //                 if(strcmp(results[i].position, current_position) == 0) {
    //                     current_position_index = i;
    //                     results[current_position_index].count++;
    //                 }
    //             }
    //
    //             if(current_position_index == -1) {
    //                 printf("Thread %d found new position: %s\n", thread_index, current_position);
    //
    //                 current_position_index = found_positions;
    //                 found_positions++;
    //
    //                 if(current_position_index >= POSITIONS) {
    //                     fprintf(stderr, "Error increased index out of array bounds");
    //                     return NULL;
    //                 }
    //
    //                 results[current_position_index].position = (char*)malloc((strlen(token) + 1));
    //                 if(results[current_position_index].position == NULL) {
    //                     perror("malloc");
    //                     return NULL;
    //                 }
    //
    //                 strcpy(results[current_position_index].position, token);
    //                 results[current_position_index].summed_age = 0;
    //                 results[current_position_index].summed_salary = 0;
    //                 results[current_position_index].count = 1;
    //             }
    //
    //             for(int i=0; i< POSITIONS;i++) {
    //                 if(strcmp(positions[i].title, current_position) == 0) {
    //                     current_position_info = positions[i];
    //                 }
    //             }
    //
    //         } else if (column == 3) {
    //             long age = strtol(token, NULL, 10);
    //             if(age != current_position_info.age) {
    //                 fprintf(stderr, "Age not matching for position %s, should be %d but is %d\n",
    //                     current_position_info.title, current_position_info.age, age);
    //             }
    //
    //             results[current_position_index].summed_age += age;
    //         } else if (column == 4) {
    //             float salary = atof(token);
    //
    //             if(salary != current_position_info.salary) {
    //                 fprintf(stderr, "Salary not matching for position %s, should be %2f but is %2f\n",
    //                         current_position_info.title, current_position_info.salary, salary);
    //             }
    //
    //             results[current_position_index].summed_salary +=salary;
    //         }
    //
    //         token = strtok_r(NULL, ",", &state);
    //         column++;
    //     }
    //
    //     if(column != 5) {
    //         fprintf(stderr, "Error in row no %d: %s\n", read_lines, line_copy);
    //         fprintf(stderr, "Number of columns should be 5 it's %d instead\n", column);
    //     }
    //
    //     memset(line, 0, LINE_SIZE);
    // }
    return read_lines;
}

int get_position_index(const struct Results* results, const char* position, const int found_positions) {
    int position_index = NOT_FOUND_POSITION;

    if(results == NULL) {
        printf("Results array is empty\n");
        return position_index;
    }

    for(int i=0; i < found_positions; i++) {
        if(i >= POSITIONS) {
            printf("Index out of bound of position array\n");
            assert(FALSE);
            return NOT_FOUND_POSITION;
        }

        if(strcmp(results[i].position, position) == 0) {
            position_index = i;
        }
    }

    return position_index;
}

int setup_new_position_data(struct Results* results, const char* position, int* found_positions, int position_index) {
    int new_position_index;
    if(position_index == -1) {
        //printf("Thread %d found new position: %s\n", thread_index, current_position);
        new_position_index = (*found_positions)++;

        if(new_position_index >= POSITIONS) {
            fprintf(stderr, "Error increased index out of array bounds");
            return -1;
        }

        results[new_position_index].position = (char*)malloc((strlen(position) + 1));
        if(results[new_position_index].position == NULL) {
            perror("malloc");
            return -1;
        }

        strcpy(results[new_position_index].position, position);
        results[new_position_index].summed_age = 0;
        results[new_position_index].summed_salary = 0;
        results[new_position_index].count = 0;
    }else {
        fprintf(stderr, "Position index should be -1 to invoke setup_new_position_data\n");
        assert(FALSE);
    }

    return new_position_index;
}

void skip_line(FILE* fp) {
    char* line[1024];
    fgets(line, sizeof(line), fp);
}

