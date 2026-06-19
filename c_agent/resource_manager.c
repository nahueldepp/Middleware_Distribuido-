#include <stdio.h>
#include <stdlib.h>

#include "resource_manager.h"

unsigned int funcion_hash(unsigned int id, unsigned int capacidad) { return id % capacidad; }

struct hash_activos * hash_init(int t){
    struct hash_activos * tabla_hash = malloc(sizeof(struct hash_activos));
    tabla_hash->capacidad = t;
    tabla_hash->tabla = calloc(t, sizeof(struct job_activo));
    return tabla_hash;
}

void resources_init(ResourceManager * rm, int cant_cpu, int cant_gpu, int cant_mem, int tam_activos){
    rm->cpu = malloc(sizeof(struct recurso_t));
    rm->gpu = malloc(sizeof(struct recurso_t));
    rm->mem = malloc(sizeof(struct recurso_t));
    rm->activos = hash_init(tam_activos);

    rm->cpu->total = rm->cpu->disponible = cant_cpu;
    rm->gpu->total = rm->gpu->disponible = cant_gpu;
    rm->mem->total = rm->mem->disponible = cant_mem;

    rm->cpu->primero = rm->cpu->ultimo = NULL;
    rm->gpu->primero = rm->gpu->ultimo = NULL;
    rm->mem->primero = rm->mem->ultimo = NULL;
}

void encolar(recurso r, unsigned int id, int socket, unsigned int rec_solicitado, unsigned int cantidad){
    struct job_pendiente * nuevo_nodo = malloc(sizeof(struct job_pendiente));
    nuevo_nodo->id = id;
    nuevo_nodo->socket = socket;
    nuevo_nodo->recurso_solicitado = rec_solicitado;
    nuevo_nodo->cantidad = cantidad;

    if (r->primero == NULL) r->primero = r->ultimo = nuevo_nodo;
    else {
        r->ultimo->siguiente = nuevo_nodo;
        r->ultimo = nuevo_nodo;
    }
}

int desencolar(recurso r, struct job_pendiente * salida){
    if (r->primero == NULL) return 0;
    *salida = *(r->primero);
    r->primero = r->primero->siguiente;
    return 1;
}
