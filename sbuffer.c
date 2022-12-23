/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#include <unistd.h>
#endif

#include "sbuffer.h"

#include "config.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

static sbuffer_node_t* create_node(const sensor_data_t* data) {
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
        .seenByDatamgr = false,
        .seenByStoragemgr = false,
    };
    return node;
}

sbuffer_t* sbuffer_create() {
    // Geen synchronisatie nodig -> niemand kan er al aan
    sbuffer_t* buffer = malloc(sizeof(sbuffer_t));
    // should never fail due to optimistic memory allocation
    assert(buffer != NULL);

    buffer->head = NULL;
    buffer->storagemgr_tail = NULL;
    buffer->datamgr_tail = NULL;
    buffer->closed = false;
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->data_available, NULL) == 0);
    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->storagemgr_tail && buffer->head == buffer->datamgr_tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->data_available) == 0);

    free(buffer);
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    // Read only
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    bool ret = buffer->closed;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return ret;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    // Write only
    assert(buffer && data);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    
    if (buffer->closed) {
        ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
        return SBUFFER_FAILURE;
    }

    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);

    // insert it
    if (buffer->head != NULL)
        // Niet empty
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->datamgr_tail == NULL) {
        // datamgr empty
        buffer->datamgr_tail = node;
    }
    if (buffer->storagemgr_tail == NULL) {
        // storagemgr empty
        buffer->storagemgr_tail = node;
    }
    

    // Terug data in de buffer -> threads wakker maken
    ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->data_available) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer, bool fromDatamgr) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    if (buffer->head == NULL) {
        // Is empty -> wachten tot nieuwe data
        ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->data_available, &buffer->mutex) == 0);
    }
    else {
        if (fromDatamgr && buffer->datamgr_tail == NULL) {
            // Datamgr heeft alles al gezien tot nu toe
            ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->data_available, &buffer->mutex) == 0);
        }
        else if (!fromDatamgr && buffer->storagemgr_tail == NULL) {
            // Storagemgr heeft alles al gezien tot nu toe
            ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->data_available, &buffer->mutex) == 0);
        }
    }
    sbuffer_node_t* removed_node;
    if (fromDatamgr) {
        removed_node = buffer->datamgr_tail;
    }
    else {
        removed_node = buffer->storagemgr_tail;
    }
    // Nu zit de laatst mogelijke node die nog niet gezien is door de datamgr/storagemgr in removed_node

    sensor_data_t ret;
    // Dit is normaal gezien altijd waar in ons geval
    if (removed_node != NULL) {
        ret = removed_node->data;

        // Komt van de datamgr en deze heeft het nog niet gezien -> nu wel dus
        if (fromDatamgr) {
            removed_node->seenByDatamgr = true;
            buffer->datamgr_tail = removed_node->prev;
        } 
        // Komt van de storagemgr en deze heeft het nog niet gezien -> nu wel dus
        else if (!fromDatamgr) {
            removed_node->seenByStoragemgr = true;
            buffer->storagemgr_tail = removed_node->prev;
        }
        // Beide hebben het gezien -> mag verwijdert worden
        if (removed_node->seenByDatamgr && removed_node->seenByStoragemgr) {
            if (removed_node == buffer->head) {
                buffer->head = NULL;
            }
            free(removed_node);
        }
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return ret;
}

void sbuffer_close(sbuffer_t* buffer) {
    assert(buffer);

    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    if (buffer->head == buffer->datamgr_tail && buffer->head == buffer->storagemgr_tail) {
        buffer->closed = true;
    }

    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
