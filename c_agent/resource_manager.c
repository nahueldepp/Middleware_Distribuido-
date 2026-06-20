#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "resource_manager.h"

int parsear_recurso(char* recurso){
    if(strcmp(recurso, "cpu") == 0) return 0;
    if(strcmp(recurso, "gpu") == 0) return 1;
    if(strcmp(recurso, "mem") == 0) return 2;
    return -1;
}

unsigned int funcion_hash(unsigned int id, int socket, unsigned int capacidad) { return (id + socket) % capacidad; }

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

struct job_activo * hash_buscar(struct hash_activos * tabla, unsigned int id, int socket){
    unsigned int i = funcion_hash(id, socket, tabla->capacidad);
    struct job_activo * actual = tabla->tabla[i];
    while (actual != NULL){
        // si tanto el socket como el id coincide, devuelve el puntero al job_activo
        if (actual->id == id && actual->socket == socket) return actual;
        actual = actual->siguiente;
    }
    // si ninguno coincide, retorna NULL
    return NULL;
}

void hash_insertar(struct hash_activos * tabla, int id, int socket, int tipo_recurso, int cantidad){
    struct job_activo * job = hash_buscar(tabla, id, socket);

    if (job == NULL){ // si el nodo no existe en la tabla, reservo memoria para el nodo y lo inicializo en 0
        job = malloc(sizeof(struct job_activo));
        job->id = id;
        job->socket = socket;
        job->cpu_asignado = job->gpu_asignado = job->mem_asignado = 0;
        unsigned int i = funcion_hash(id, socket, tabla->capacidad);
        job->siguiente = tabla->tabla[i];
        tabla->tabla[i] = job;
    }
    // tanto si hash_buscar encontro o no, le sumo la cantidad del recurso solicitado.
    if (tipo_recurso == 0) job->cpu_asignado += cantidad;
    else if (tipo_recurso == 1) job->gpu_asignado += cantidad;
    else if (tipo_recurso == 2) job->mem_asignado += cantidad;
}

// devuelve 1 si el trabajo fue concedido, 0 si fue encolado y -1 en caso de error
int handler_reserve(ResourceManager * rm, int socket, char* string_id, char* string_recurso, char* string_cantidad){
    int id = atoi(string_id);
    int cantidad = atoi(string_cantidad);
    int rec = parsear_recurso(string_recurso);

    recurso r = NULL;
    if (rec == -1){ printf("Error: recurso incorrecto\n"); return -1; }
    else if (rec == 0) r = rm->cpu;
    else if (rec == 1) r = rm->gpu;
    else if (rec == 2) r = rm->mem;

    if (r->total < cantidad) { printf("Error: cantidad de recurso solicitado mayor al total\n"); return -1; }

    else if (r->disponible >= cantidad){
        r->disponible -= cantidad;
        hash_insertar(rm->activos, id, socket, rec, cantidad);
        return 1;
    }

    else {
        encolar(r, id, socket, rec, cantidad);
        return 0;
    }
}

// devuelve 0 en caso de tener exito y -1 en caso de error
int handler_release(ResourceManager * rm, int socket, char* string_id, char* string_recurso, char* string_cantidad){
    int id = atoi(string_id);
    int cantidad = atoi(string_cantidad);
    int rec = parsear_recurso(string_recurso);

    int i = funcion_hash(id, socket, rm->activos->capacidad);
    struct job_activo * job = rm->activos->tabla[i];

    if (job == NULL) { printf("Error: JOB inexistente\n"); return -1; }

    else if (job->socket == socket && job->id == id){
        if (rec == -1){ printf("Error: recurso incorrecto\n"); return -1; }
        else if (rec == 0) job->cpu_asignado -= cantidad;
        else if (rec == 1) job->gpu_asignado -= cantidad;
        else if (rec == 2) job->mem_asignado -= cantidad;

        if (job->cpu_asignado == 0 && job->gpu_asignado == 0 && job->mem_asignado == 0){
            rm->activos->tabla[i] = job->siguiente;
            free(job);
            return 0;
        }
        return 0;
    }

    else {
        struct job_activo * anterior = job;
        struct job_activo * actual = anterior->siguiente;

        while (actual != NULL && (actual->id != id || actual->socket != socket)){
            anterior = actual;
            actual = actual->siguiente;
        }

        if (actual == NULL) { printf("Error: JOB inexistente\n"); return -1;}

        if (rec == -1){ printf("Error: recurso incorrecto\n"); return -1; }
        else if (rec == 0) actual->cpu_asignado -= cantidad;
        else if (rec == 1) actual->gpu_asignado -= cantidad;
        else if (rec == 2) actual->mem_asignado -= cantidad;

        if (actual->cpu_asignado == 0 && actual->gpu_asignado == 0 && actual->mem_asignado == 0){
            anterior->siguiente = actual->siguiente;
            free(actual);
            return 0;
        }
        return 0;
    }
}