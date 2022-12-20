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

// Beide moeten in de header file gemaakt worden, anders kan de conmgr niet de cond var signallen
typedef struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
    bool seenByDatamgr;
    bool seenByStoragemgr;
} sbuffer_node_t;

typedef struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    bool closed;
    pthread_mutex_t mutex;
    pthread_cond_t cond_datamgr;
    pthread_cond_t cond_storagemgr;
} sbuffer_t;

/**
 * Allocate and initialize a new shared buffer
 */
sbuffer_t* sbuffer_create();

/**
 * Clean up & free all allocated resources
 */
void sbuffer_destroy(sbuffer_t* buffer);

bool sbuffer_is_empty(sbuffer_t* buffer);

bool sbuffer_is_closed(sbuffer_t* buffer);

/*
    Gain/release exclusive access to the buffer
    TODO: these functions should not exist!
        All buffer synchronization should be
        internal to the buffer, users should not
        be concerned with it!
*/
void sbuffer_lock(sbuffer_t* buffer);
void sbuffer_unlock(sbuffer_t* buffer);

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
