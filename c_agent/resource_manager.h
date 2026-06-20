#ifndef RESOURCE_MANAGER_H
#define RESOURCE_MANAGER_H

struct job_pendiente {
    unsigned int id;
    int socket;
    unsigned int recurso_solicitado; // cpu = 0, gpu = 1, mem = 2
    unsigned int cantidad;
    struct job_pendiente * siguiente;
};

struct job_activo {
    unsigned int id;
    int socket;
    unsigned int cpu_asignado;
    unsigned int gpu_asignado;
    unsigned int mem_asignado;
    struct job_activo * siguiente;
};

struct hash_activos {
    struct job_activo ** tabla;
    unsigned int capacidad;
};

typedef struct recurso_t {
    unsigned int total;
    unsigned int disponible;
    struct job_pendiente * primero;
    struct job_pendiente * ultimo;
} * recurso;

typedef struct {
    recurso cpu;
    recurso gpu;
    recurso mem;
    struct hash_activos * activos;
} ResourceManager;

#endif