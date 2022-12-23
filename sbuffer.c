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
    buffer->tail = NULL;
    buffer->closed = false;
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->cond_datamgr, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->cond_storagemgr, NULL) == 0);
    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->cond_datamgr) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->cond_storagemgr) == 0);

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
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->tail == NULL)
        buffer->tail = node;

    // Terug data in de buffer -> threads wakker maken
    ASSERT_ELSE_PERROR(pthread_cond_signal(&buffer->cond_datamgr) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_signal(&buffer->cond_storagemgr) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer, bool fromDatamgr) {
    assert(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    // if (buffer->head == NULL) {
    //     // Is empty -> wachten tot nieuwe data
    //     if (fromDatamgr) {
    //         ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->cond_datamgr, &buffer->mutex) == 0);
    //     } else {
    //         ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->cond_storagemgr, &buffer->mutex) == 0);
    //     }
    // }

    sbuffer_node_t* removed_node = buffer->tail;
    int count = 1;
    if (fromDatamgr) {
        // Terugkeren in de buffer zolang we niet in het begin zijn of totdat we een nog niet door de datamgr bekeken node tegenkomen
        while (true) {
            if (removed_node == NULL) {
                // De datamgr heeft tot nu toe alles gezien -> wachten op nieuwe data
                ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->cond_datamgr, &buffer->mutex) == 0);
                // Nu kunnen we de eerste node nemen die zonet is toegevoegd
                removed_node = buffer->head;
                break;
            }
            else {
                if (!removed_node->seenByDatamgr) {
                    // Hoera, een node gevonden die de datamgr nog niet heeft gezien! -> verder gaan in de functie
                    break;
                }
                // Vorige node eens proberen
                removed_node = removed_node->prev;
                count++;
            }
        }
    } 
    else {
        // Terugkeren in de buffer zolang we niet in het begin zijn of totdat we een nog niet door de storagemgr bekeken node tegenkomen
        while (true) {
            if (removed_node == NULL) {
                // De storagemgr heeft tot nu toe alles gezien -> wachten op nieuwe data
                ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->cond_storagemgr, &buffer->mutex) == 0);
                // Nu kunnen we de eerste node nemen die zonet is toegevoegd
                removed_node = buffer->head;
                break;
            }
            else {
                if (!removed_node->seenByStoragemgr) {
                    // Hoera, een node gevonden die de storagemgr nog niet heeft gezien! -> verder gaan in de functie
                    break;
                }
                // Vorige node eens proberen
                removed_node = removed_node->prev;
                count++;
            }
        }
    }
    // Nu zit de laatst mogelijke node die nog niet gezien is door de datamgr/storagemgr in removed_node

    if (fromDatamgr) {
        printf("Datamgr heeft node gevonden na %d keer.\n", count);
    } else {
        printf("Storagemgr heeft node gevonden na %d keer.\n", count);
    }

    sensor_data_t ret;
    // Dit is normaal gezien altijd waar in ons geval
    if (removed_node != NULL) {
        ret = removed_node->data;

        // Komt van de datamgr en deze heeft het nog niet gezien -> nu wel dus
        if (fromDatamgr) {
            removed_node->seenByDatamgr = true;
        } 
        // Komt van de storagemgr en deze heeft het nog niet gezien -> nu wel dus
        else if (!fromDatamgr) {
            removed_node->seenByStoragemgr = true;
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
        // door beide gezien -> komt nooit voor door sbuffer_remove_last, deze verwijderd zodra beide gelezen hebben
        // door 1 thread niet gezien -> mag nog niet dicht
    }
    else {
        // Buffer is wel leeg -> OK
      buffer->closed = true;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
}
