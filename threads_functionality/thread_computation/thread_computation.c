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

#define LINE_SIZE 1024

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

void* compute_on_thread(void* arg)
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

    char line[LINE_SIZE];
    char line_copy[LINE_SIZE];

    int read_lines = 0;
    int allocations = 0;

    int found_positions = 0;

    fgets(line, sizeof(line), fp);

    skip_line(fp);
    while(ftell(fp) <= end_for_thread && fgets(line, sizeof(line), fp)) {
        read_lines++;

        line[strcspn(line, "\n")] = '\0';
        strcpy(line_copy, line);

        int current_position_index = -1;
        char* token, *state;
        int column = 0;

        token = strtok_r(line, ",", &state);
        PositionInfo current_position_info;
        while(token != NULL && column < 5) {

            if(column == 2) {
                char* current_position = token;

                for(int i=0; i < found_positions; i++) {
                    if(strcmp(results[i].position, current_position) == 0) {
                        current_position_index = i;
                        results[current_position_index].count++;
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
                    results[current_position_index].count = 1;
                }

                for(int i=0; i< POSITIONS;i++) {
                    if(strcmp(positions[i].title, current_position) == 0) {
                        current_position_info = positions[i];
                    }
                }

            } else if (column == 3) {
                long age = strtol(token, NULL, 10);
                if(age != current_position_info.age) {
                    fprintf(stderr, "Age not matching for position %s, should be %d but is %d\n",
                        current_position_info.title, current_position_info.age, age);
                }

                results[current_position_index].summed_age += age;
            } else if (column == 4) {
                float salary = atof(token);

                if(salary != current_position_info.salary) {
                    fprintf(stderr, "Salary not matching for position %s, should be %2f but is %2f\n",
                            current_position_info.title, current_position_info.salary, salary);
                }

                results[current_position_index].summed_salary +=salary;
            }

            token = strtok_r(NULL, ",", &state);
            column++;
        }

        if(column != 5) {
            fprintf(stderr, "Error in row no %d: %s\n", read_lines, line_copy);
            fprintf(stderr, "Number of columns should be 5 it's %d instead\n", column);
        }

        memset(line, 0, LINE_SIZE);
    }

    fclose(fp);
    printf("Thread %d has read %d lines\n", thread_index, read_lines);

    return results;
}

void skip_line(FILE* fp) {
    char* line[1024];
    fgets(line, sizeof(line), fp);
}

