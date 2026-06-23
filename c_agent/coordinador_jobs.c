/*Para  JOB_REQUEST compuesto  coordiné,  recursos concedidos que quedaron repartidos en  tales nodos*/
#include "coordinador_jobs.h"

#include <string.h>

void coordinator_jobs_init(CoordinatorJobTable *table) {
    memset(table, 0, sizeof(*table));
}

CoordinatedJob *coordinator_jobs_get(CoordinatorJobTable *table, int job_id) {
    for (int i = 0; i < MAX_COORD_JOBS; i++) {
        if (table->jobs[i].activo && table->jobs[i].job_id == job_id) {
            return &table->jobs[i];
        }
    }

    return NULL;
}

int coordinator_jobs_create(CoordinatorJobTable *table, int job_id) {
    if (coordinator_jobs_get(table, job_id) != NULL) {
        return 0;
    }

    for (int i = 0; i < MAX_COORD_JOBS; i++) {
        if (!table->jobs[i].activo) {
            table->jobs[i].activo = 1;
            table->jobs[i].job_id = job_id;
            table->jobs[i].cant_recursos = 0;
            return 1;
        }
    }

    return 0;
}

int coordinator_jobs_add_resource(CoordinatorJobTable *table,
                                  int job_id,
                                  const char *ip,
                                  int puerto,
                                  const char *recurso,
                                  int cantidad,
                                  int es_local) {
    CoordinatedJob *job = coordinator_jobs_get(table, job_id);

    if (job == NULL) {
        return 0;
    }

    if (job->cant_recursos >= MAX_JOB_RESOURCES) {
        return 0;
    }

    CoordinatedResource *r = &job->recursos[job->cant_recursos];

    strncpy(r->ip, ip, MAX_IP_LEN - 1);
    r->ip[MAX_IP_LEN - 1] = '\0';

    strncpy(r->recurso, recurso, MAX_RESOURCE_LEN - 1);
    r->recurso[MAX_RESOURCE_LEN - 1] = '\0';

    r->puerto = puerto;
    r->cantidad = cantidad;
    r->es_local = es_local;

    job->cant_recursos++;

    return 1;
}

int coordinator_jobs_remove(CoordinatorJobTable *table, int job_id) {
    CoordinatedJob *job = coordinator_jobs_get(table, job_id);

    if (job == NULL) {
        return 0;
    }

    memset(job, 0, sizeof(*job));

    return 1;
}