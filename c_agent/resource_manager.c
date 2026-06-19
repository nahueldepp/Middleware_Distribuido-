#include <stdio.h>
#include <stdlib.h>

struct job_pendiente {
    int id;
    int socket;
    int recurso_solicitado; // cpu = 0, gpu = 1, mem = 2
    int cantidad;
    struct job_pendiente * siguiente;
};

struct job_activo {
    int id;
    int socket;
    int cpu_asignado;
    int gpu_asignado;
    int mem_asignado;
    struct job_activo * siguiente;
};

struct hash_activos {
    struct job_activo ** tabla;
    int capacidad;
};

typedef struct recurso_t {
    int total;
    int disponible;
    struct job_pendiente * primero;
    struct job_pendiente * ultimo;
} * recurso;

typedef struct resman {
    recurso cpu;
    recurso gpu;
    recurso mem;
} ResourceManager;

void resources_init(ResourceManager * rm, int cant_cpu, int cant_gpu, int cant_mem){
    rm->cpu = malloc(sizeof(struct recurso_t));
    rm->gpu = malloc(sizeof(struct recurso_t));
    rm->mem = malloc(sizeof(struct recurso_t));

    rm->cpu->total = rm->cpu->disponible = cant_cpu;
    rm->gpu->total = rm->gpu->disponible = cant_gpu;
    rm->mem->total = rm->mem->disponible = cant_mem;

    rm->cpu->primero = rm->cpu->ultimo = NULL;
    rm->gpu->primero = rm->gpu->ultimo = NULL;
    rm->mem->primero = rm->mem->ultimo = NULL;
}