#ifndef COORDINATOR_JOBS_H
#define COORDINATOR_JOBS_H

#define MAX_COORD_JOBS 128
#define MAX_JOB_RESOURCES 32
#define MAX_IP_LEN 64
#define MAX_RESOURCE_LEN 32

/*Es para guardar el resultado final de un job ya concedido
Indica jobs activos que esperan un release
parecida a Reserva parcial pero se me ocurrio dsp
*/
typedef struct {
    char ip[MAX_IP_LEN];
    int puerto;
    char recurso[MAX_RESOURCE_LEN];
    int cantidad;
    int es_local;
} CoordinatedResource;

typedef struct {
    int job_id;
    int activo;

    CoordinatedResource recursos[MAX_JOB_RESOURCES];
    int cant_recursos;
} CoordinatedJob;

typedef struct {
    CoordinatedJob jobs[MAX_COORD_JOBS];
} CoordinatorJobTable;

void coordinator_jobs_init(CoordinatorJobTable *table);

CoordinatedJob *coordinator_jobs_get(CoordinatorJobTable *table, int job_id);

int coordinator_jobs_create(CoordinatorJobTable *table, int job_id);

int coordinator_jobs_add_resource(CoordinatorJobTable *table,
                                  int job_id,
                                  const char *ip,
                                  int puerto,
                                  const char *recurso,
                                  int cantidad,
                                  int es_local);

int coordinator_jobs_remove(CoordinatorJobTable *table, int job_id);

#endif