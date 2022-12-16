/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "sbuffer.h"

#include "config.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

typedef struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
    bool seenByDatamgr;
    bool seenByStoragemgr;
} sbuffer_node_t;

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    bool closed;
    pthread_mutex_t mutex;
};

static sbuffer_node_t* create_node(const sensor_data_t* data) {
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
    };
    return node;
}

sbuffer_t* sbuffer_create() {
    // Geen synchronisatie nodig -> niemand kan er al aan
    sbuffer_t* buffer = malloc(sizeof(sbuffer_t));
    // should never fail due to optimistic memory allocation
    assert(buffer != NULL);

    buffer->head = NULL;
    buffer->tail = NULL;
    buffer->closed = false;
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);

    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    free(buffer);
}

// void sbuffer_lock(sbuffer_t* buffer) {
//     assert(buffer);
//     ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
// }
// 
// void sbuffer_unlock(sbuffer_t* buffer) {
//     assert(buffer);
//     ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
// }

bool sbuffer_is_empty(sbuffer_t* buffer) {
    // Read only
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    return buffer->head == NULL;
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    // Read only
    assert(buffer);
    bool ret = buffer->closed;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return ret;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    // Write only
    assert(buffer && data);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    if (buffer->closed)
        return SBUFFER_FAILURE;

    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);
    node->seenByDatamgr = false;
    node->seenByStoragemgr = false;

    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->tail == NULL)
        buffer->tail = node;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer, bool fromDatamgr) {
    assert(buffer);
    assert(buffer->head != NULL);
    
    sbuffer_node_t* removed_node = buffer->tail;
    assert(removed_node != NULL);
    sensor_data_t ret = removed_node->data;

    // Komt van de datamgr en deze heeft het nog niet gezien -> nu wel dus
    if (fromDatamgr && !buffer->tail->seenByDatamgr) {
        buffer->tail->seenByDatamgr = true;
    } 
    // Komt van de storagemgr en deze heeft het nog niet gezien -> nu wel dus
    else if (!fromDatamgr && !buffer->tail->seenByStoragemgr) {
        buffer->tail->seenByStoragemgr = true;
    }
    // Beide hebben het gezien -> mag verwijdert worden
    if (buffer->tail->seenByDatamgr && buffer->tail->seenByStoragemgr) {
        if (removed_node == buffer->head) {
            buffer->head = NULL;
            assert(removed_node == buffer->tail);
        }
        buffer->tail = removed_node->prev;
        free(removed_node);
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return ret;
}

void sbuffer_close(sbuffer_t* buffer) {
    assert(buffer);

    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    if (buffer->head != buffer->tail) {
        // Buffer is niet leeg
        if (!buffer->tail->seenByDatamgr && !buffer->tail->seenByStoragemgr) {
            // Laatste data is niet door een thread gezien -> OK
            buffer->closed = true;
        }
        // else
        // door beide gezien -> komt nooit voor door sbuffer_remove_last, deze verwijdert zodra beide gelezen hebben
        // door 1 thread niet gezien -> mag nog niet dicht
    }
    else {
        // Buffer is wel leeg -> OK
      buffer->closed = true;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
