/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "config.h"
#include "connmgr.h"
#include "datamgr.h"
#include "sbuffer.h"
#include "sensor_db.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <wait.h>

static int print_usage() {
    printf("Usage: <command> <port number> \n");
    return -1;
}

typedef struct run_manager_args {
    sbuffer_t* buffer;
    bool fromDatamgr; 
} run_manager_args_t;

void* run_manager(void* _args) {
    // void pointer -> struct pointer
    run_manager_args_t *args = (run_manager_args_t *) _args;
    DBCONN* db = NULL;
    if (args->fromDatamgr) {
        datamgr_init();
    } else {
        db = storagemgr_init_connection(1);
        assert(db != NULL);
    }

    while (true) {
        sensor_data_t data = sbuffer_remove_last(args->buffer, args->fromDatamgr);
        if (args->fromDatamgr) {
            datamgr_process_reading(&data);
        } else {
            storagemgr_insert_sensor(db, data.id, data.value, data.ts);
        }

        if (sbuffer_is_closed(args->buffer)) {
            break;
        }
    }
    
    if (args->fromDatamgr) {
        datamgr_free();
    } else {
        storagemgr_disconnect(db);
    }
    return NULL;
}

// static void* datamgr_run(void* buffer) {
//     datamgr_init();
// 
//     // datamgr loop
//     while (true) {
//         // sbuffer_lock(buffer);
//         sensor_data_t data = sbuffer_remove_last(buffer, true);
//         datamgr_process_reading(&data);
//         // everything nice & processed
//         if (sbuffer_is_closed(buffer)) {
//             // buffer is both empty & closed: there will never be data again
//             // sbuffer_unlock(buffer);
//             break;
//         }
//         // give the others a chance to lock the mutex
//         // sbuffer_unlock(buffer);
//     }
// 
//     datamgr_free();
// 
//     return NULL;
// }

//static void* storagemgr_run(void* buffer) {
//    DBCONN* db = storagemgr_init_connection(1);
//    assert(db != NULL);
//
//    // storagemgr loop
//    while (true) {
//        // sbuffer_lock(buffer);
//        sensor_data_t data = sbuffer_remove_last(buffer, false);
//        storagemgr_insert_sensor(db, data.id, data.value, data.ts);
//
//        // everything nice & processed
//        if (sbuffer_is_closed(buffer)) {
//            // buffer is both empty & closed: there will never be data again
//            // sbuffer_unlock(buffer);
//            break;
//        }
//        // give the others a chance to lock the mutex
//        // sbuffer_unlock(buffer);
//    }
//
//    storagemgr_disconnect(db);
//    return NULL;
//}

int main(int argc, char* argv[]) {
    if (argc != 2)
        return print_usage();
    char* strport = argv[1];
    char* error_char = NULL;
    int port_number = strtol(strport, &error_char, 10);
    if (strport[0] == '\0' || error_char[0] != '\0')
        return print_usage();

    sbuffer_t* buffer = sbuffer_create();

    pthread_t datamgr_thread;
    run_manager_args_t datamgr_args;
    datamgr_args.fromDatamgr = true;
    datamgr_args.buffer = buffer;
    ASSERT_ELSE_PERROR(pthread_create(&datamgr_thread, NULL, run_manager,  &datamgr_args) == 0);

    pthread_t storagemgr_thread;
    run_manager_args_t storagemgr_args;
    storagemgr_args.fromDatamgr = false;
    storagemgr_args.buffer = buffer;
    ASSERT_ELSE_PERROR(pthread_create(&storagemgr_thread, NULL, run_manager, &storagemgr_args) == 0);

    // main server loop
    connmgr_listen(port_number, buffer);

    sbuffer_close(buffer);

    pthread_join(datamgr_thread, NULL);
    pthread_join(storagemgr_thread, NULL);

    sbuffer_destroy(buffer);

    wait(NULL);

    return 0;
}