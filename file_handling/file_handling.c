//
// Created by karol on 10/7/24.
//
#include "file_handling.h"
#include <stdio.h>
#include <stdlib.h>

FILE* open_file_for_reading(const char* file_name)
{
    FILE* file = fopen(file_name, "r");
    if(file == NULL) {
        fprintf(stderr, "Error opening file %s\n", file_name);
        perror("fopen");
        return NULL;
    }

    return file;
}