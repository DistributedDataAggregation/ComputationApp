//
// Created by karol on 10/7/24.
//

#define _GNU_SOURCE
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "thread_computation.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "../thread_argument.h"
#include "../../file_handling/file_handling.h"
#include "../../consts/POSITIONS.h"
#include "../results.h"

void skip_line(FILE* fp);

void* test_thread(void* arg)
{

    struct Results* results = malloc(sizeof(struct Results)*POSITIONS);

    struct ThreadArgs* thread_args = (struct ThreadArgs*)arg;
    int thread_index = thread_args->thread_index;

    printf("Thread number %d started\n", thread_index);
    printf("Filename %s\n", thread_args->filename);

    FILE* fp = open_file_for_reading(thread_args->filename);
    if(fp == NULL) {
        printf("Thread %d is quiting because of failed file opening attempt\n", thread_index);
        return NULL;
    }

    struct stat file_stat;
    fstat(fileno(fp), &file_stat);

    // be careful with this, file size isn't always going to be nicely dividible by threads_count
    // we don't want to skip any line by mistake cause of division problems;
    long bytes_per_thread = (file_stat.st_size)/thread_args->threads_count;
    long start_for_thread = bytes_per_thread*thread_index;
    long end_for_thread = start_for_thread + bytes_per_thread;

    if(fseek(fp, start_for_thread, SEEK_SET) != 0) {
        perror("fseek");
        return NULL;
    }

    printf("Thread %d has opened the file for reading\n", thread_index);

    char line[1024];

    int read_lines = 0;
    int allocations = 0;

    int found_positions = 0;

    fgets(line, sizeof(line), fp);

    skip_line(fp);
    while(ftell(fp) < end_for_thread && fgets(line, sizeof(line), fp)) {
        read_lines++;
        line[strcspn(line, "\n")] = '\0';

        int current_position_index = -1;
        char* token;
        int column = 0;

        token = strtok(line, ",");
        while(token != NULL && column < 5) {

            if(column == 2) {
                char* current_position = token;

                for(int i=0; i < found_positions; i++) {
                    if(strcmp(results[i].position, current_position) == 0) {
                        current_position_index = i;
                    }
                }

                if(current_position_index == -1) {
                    printf("Thread %d found new position: %s\n", thread_index, current_position);

                    current_position_index = found_positions;
                    found_positions++;

                    if(current_position_index >= POSITIONS) {
                        fprintf(stderr, "Error increased index out of array bounds");
                        return NULL;
                    }

                    results[current_position_index].position = (char*)malloc((strlen(token) + 1));
                    if(results[current_position_index].position == NULL) {
                        perror("malloc");
                        return NULL;
                    }

                    strcpy(results[current_position_index].position, token);
                    results[current_position_index].summed_age = 0;
                    results[current_position_index].summed_salary = 0;
                    results[current_position_index].count = 0;
                }

            } else if (column == 3) {
                //long age = strtol(token, NULL, 10);
                //results[current_position_index].summed_age += age;
            } else if (column == 4) {
                //float salary = atof(token);
                //results[current_position_index].summed_salary +=salary;
            }

            token = strtok(NULL, ",");
            column++;
        }

    }

    fclose(fp);
    printf("Thread %d has read %d lines\n", thread_index, read_lines);

    return NULL;
}

void skip_line(FILE* fp) {
    char* line[1024];
    fgets(line, sizeof(line), fp);
}

