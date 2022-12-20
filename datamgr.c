/**
 * \author Mathieu Erbas
 */

#include "datamgr.h"

#include "lib/vector.h"

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// Definitions for HVAC Control

#if !defined RUN_AVG_LENGTH
    #define RUN_AVG_LENGTH 5
#endif

#if !defined(SET_MIN_TEMP)
    #define SET_MIN_TEMP 20
#endif

#if !defined SET_MAX_TEMP
    #define SET_MAX_TEMP 25
#endif

#define BOLD "\x1B[1m"
#define RED "\x1B[31m"
#define RESET "\x1B[0m"


typedef struct {
    uint16_t sensor_id;
    time_t last_modified;
    double buffer[RUN_AVG_LENGTH];
    unsigned count;
} sensor_t;

static vector_t* sensors = NULL;

static sensor_value_t sensor_running_average(sensor_t* sensor) {
    sensor_value_t sum = 0;
    for (int i = 0; i < RUN_AVG_LENGTH; i++) {
        sum += sensor->buffer[i];
    }
    return sum / RUN_AVG_LENGTH;
}

static bool sensor_equals(void* s1, void* s2) {
    return ((sensor_t*) s1)->sensor_id == ((sensor_t*) s2)->sensor_id;
}

static sensor_t* datamgr_find_sensor(uint16_t sensor_id) {
    sensor_t sensor = {.sensor_id = sensor_id};
    return vector_find(sensors, &sensor, sensor_equals);
}

void datamgr_init() {
    sensors = vector_create();
    assert(sensors);
}

void datamgr_process_reading(const sensor_data_t* data) {
    sensor_t* obtained_sensor = datamgr_find_sensor(data->id);
    if (!obtained_sensor) { // sensor with id not found
        printf("Received sensor data with new sensor node id %d \n", data->id);
        // put new sensor in sensor list
        obtained_sensor = calloc(1, sizeof(*obtained_sensor)); // initialize to zero
        obtained_sensor->sensor_id = data->id;
        vector_add(sensors, obtained_sensor);
    }

    obtained_sensor->last_modified = data->ts;
    obtained_sensor->buffer[obtained_sensor->count % RUN_AVG_LENGTH] = data->value;
    obtained_sensor->count++;

    sensor_value_t running_average = sensor_running_average(obtained_sensor);
    if (obtained_sensor->count >= RUN_AVG_LENGTH) {
        if (running_average < SET_MIN_TEMP) {
            printf(BOLD RED "Sensor %" PRIu16 " read a temperature value (%f) lower than " TO_STRING(SET_MIN_TEMP) "\n" RESET, data->id, data->value);
        }
        if (running_average > SET_MAX_TEMP) {
            printf(BOLD RED "Sensor %" PRIu16 " read a temperature value (%f) higher than " TO_STRING(SET_MAX_TEMP) "\n" RESET, data->id, data->value);
        }
    }
}

void datamgr_free() {
    for (size_t i = 0; i < vector_size(sensors); i++)
        free(vector_at(sensors, i));
    vector_destroy(sensors);
}
