#pragma once

/**
 * \author Mathieu Erbas
 */

#include <pthread.h>
#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "config.h"

#define SBUFFER_FAILURE -1
#define SBUFFER_SUCCESS 0

typedef struct sbuffer sbuffer_t;

/**
 * Allocate and initialize a new shared buffer
 */
sbuffer_t* sbuffer_create();

/**
 * Clean up & free all allocated resources
 */
void sbuffer_destroy(sbuffer_t* buffer);

bool sbuffer_is_closed(sbuffer_t* buffer);


/**
 * Inserts the sensor data in 'data' at the start of 'buffer' (at the 'head')
 * \param buffer a pointer to the buffer that is used
 * \param data a pointer to sensor_data_t data, that will be _copied_ into the buffer
 * \return the current status of the buffer
 */
int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data);

/**
 * Removes & returns the last measurement in the buffer (at the 'tail')
 * \return the removed measurement
 */
sensor_data_t sbuffer_remove_last(sbuffer_t* buffer, bool fromDatamgr);

/**
 * Closes the buffer. This signifies that no more data will be inserted.
 */
void sbuffer_close(sbuffer_t* buffer);
