#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "resource_manager.h"

// funcion auxiliar que dado un string, devuelve el entero correspondiente que representa el recurso o -1 en caso de error
static int parsear_recurso(char* recurso){
    if(strcmp(recurso, "cpu") == 0) return 0;
    if(strcmp(recurso, "gpu") == 0) return 1;
    if(strcmp(recurso, "mem") == 0) return 2;
    return -1;
}

// funcion auxiliar que dada la estructura ResourceManager, un entero que representa el recurso y un entero "cantidad", suma la cantidad al recurso dentro de la estructura
static void sumar_recurso(ResourceManager * rm, unsigned int rec, unsigned int cantidad){
    if (rec == 0) rm->cpu->disponible += cantidad;
    else if (rec == 1) rm->gpu->disponible += cantidad;
    else  rm->mem->disponible += cantidad;
}

// funcion hash compuesta
static unsigned int funcion_hash(unsigned int id, int socket, unsigned int capacidad) { return (id + socket) % capacidad; }

// hash_init: dado un entero tamaño, reserva memoria para la tabla hash y devuelve el puntero a la tabla
static struct hash_activos * hash_init(unsigned int t){
    struct hash_activos * tabla_hash = malloc(sizeof(struct hash_activos));
    tabla_hash->capacidad = t;
    tabla_hash->tabla = calloc(t, sizeof(struct job_activo *));
    return tabla_hash;
}

// resources_init: toma un puntero a ResourceManager, la cantidad total de cpu, gpu y mem y el tamaño de la tabla y reserva memoria para cada componente del ResourceManager
void resources_init(ResourceManager * rm, unsigned int cant_cpu, unsigned int cant_gpu, unsigned int cant_mem, unsigned int tam_activos){
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

// encolar: funcion FIFO de insercion de la cola. crea un nuevo nodo y lo insterta al final de la cola
static void encolar(recurso r, unsigned int id, int socket, unsigned int rec_solicitado, unsigned int cantidad){
    struct job_pendiente * nuevo_nodo = malloc(sizeof(struct job_pendiente));
    nuevo_nodo->id = id;
    nuevo_nodo->socket = socket;
    nuevo_nodo->recurso_solicitado = rec_solicitado;
    nuevo_nodo->cantidad = cantidad;
    nuevo_nodo->siguiente = NULL;

    if (r->primero == NULL) r->primero = r->ultimo = nuevo_nodo;
    else {
        r->ultimo->siguiente = nuevo_nodo;
        r->ultimo = nuevo_nodo;
    }
}

// desencolar: funcion FIFO de eliminacion en la cola. libera la memoria del primer nodo en la cola y pone al segundo nodo como nuevo primero
static int desencolar(recurso r){
    if (r->primero == NULL) return -1;
    else {
        struct job_pendiente* a_borrar = r->primero;
        r->primero = r->primero->siguiente;
        if (r->primero == NULL) r->ultimo = NULL;
        free(a_borrar);
        return 0;
    }
}

// hash_buscar: dadas la tabla hash, el id y socket, devuelve un puntero al nodo que coincide o NULL si no existe
static struct job_activo * hash_buscar(struct hash_activos * tabla, unsigned int id, int socket){
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

// hash_insertar: dada la tabla hash, el socket, id, recurso y cantidad, crea un nuevo nodo y lo inserta el la tabla hash
// o suma la cantidad al recurso correspondiente en caso de que el mismo socket ya tenga un job con el mismo id ya insertado
static void hash_insertar(struct hash_activos * tabla, unsigned int id, int socket, unsigned int tipo_recurso, unsigned int cantidad){
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

// handler_reserve: funcion principal encargada de manejar un reserve.
// devuelve 1 si el trabajo fue concedido, 0 si fue encolado y -1 en caso de error
int handler_reserve(ResourceManager * rm, int socket, char* string_id, char* string_recurso, char* string_cantidad){
    unsigned int id = atoi(string_id);
    unsigned int cantidad = atoi(string_cantidad);
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

// handler_release: funcion principal encargada de manejar un release. al final de la ejecucion atiende a los jobs pendientes encolados en cada recurso.
// devuelve 0 en caso de tener exito y -1 en caso de error
int handler_release(ResourceManager * rm, int socket, char* string_id, char* string_recurso, char* string_cantidad, Notificacion* notificaciones, int* cant_notificaciones, int cant_max){
    unsigned int id = atoi(string_id);
    int cantidad = atoi(string_cantidad);
    int rec = parsear_recurso(string_recurso);
    
    // Bandera para saber si es la liberación total del JOB_RELEASE de Erlang
    int liberacion_total = (strcmp(string_recurso, "todo") == 0);

    // Modificamos las validaciones: si es liberación total, ignoramos cantidad y recurso
    if (!liberacion_total && cantidad <= 0) { printf("Error: cantidad invalida\n"); return -1; }
    if (!liberacion_total && rec == -1) { printf("Error: recurso inexistente\n"); return -1; }

    unsigned int i = funcion_hash(id, socket, rm->activos->capacidad);
    struct job_activo * job = rm->activos->tabla[i];

    if (job == NULL) { printf("Error: JOB inexistente\n"); return -1; }

    struct job_activo * actual = NULL;
    struct job_activo * anterior_nodo = NULL;

    if (job->socket == socket && job->id == id) {
        actual = job;
    } else {
        anterior_nodo = job;
        actual = job->siguiente;
        while (actual != NULL && (actual->id != id || actual->socket != socket)){
            anterior_nodo = actual;
            actual = actual->siguiente;
        }
    }

    if (actual == NULL) { printf("Error: JOB inexistente\n"); return -1; }

    // Variables locales para acumular qué devolvemos al pozo general
    unsigned int cpu_devuelto = 0;
    unsigned int gpu_devuelto = 0;
    unsigned int mem_devuelto = 0;

    if (liberacion_total) {
        // Guardamos todo lo asignado para sumarlo después a los disponibles
        cpu_devuelto = actual->cpu_asignado;
        gpu_devuelto = actual->gpu_asignado;
        mem_devuelto = actual->mem_asignado;

        actual->cpu_asignado = 0;
        actual->gpu_asignado = 0;
        actual->mem_asignado = 0;

        // Sumamos físicamente a la disponibilidad del ResourceManager
        rm->cpu->disponible += cpu_devuelto;
        rm->gpu->disponible += gpu_devuelto;
        rm->mem->disponible += mem_devuelto;
    } else {
        if (rec == 0) { 
            if(actual->cpu_asignado < (unsigned int)cantidad) return -1; 
            actual->cpu_asignado -= cantidad;
        }
        else if (rec == 1) { 
            if(actual->gpu_asignado < (unsigned int)cantidad) return -1; 
            actual->gpu_asignado -= cantidad;
        }
        else if (rec == 2) { 
            if(actual->mem_asignado < (unsigned int)cantidad) return -1; 
            actual->mem_asignado -= cantidad;
        }
        sumar_recurso(rm, rec, cantidad);
    }

    // Si el job se quedó sin recursos asignados, lo removemos de la estructura hash
    if (actual->cpu_asignado == 0 && actual->gpu_asignado == 0 && actual->mem_asignado == 0) {
        if (actual == rm->activos->tabla[i]) {
            rm->activos->tabla[i] = actual->siguiente;
        } else if (anterior_nodo != NULL) {
            anterior_nodo->siguiente = actual->siguiente;
        }
        free(actual);
    }

    // Revisa secuencialmente las 3 colas de recursos para activar los jobs pendientes.
    recurso recursos_a_revisar[3] = {rm->cpu, rm->gpu, rm->mem};

    for (int j = 0; j < 3; j++) {
        recurso r = recursos_a_revisar[j];
        while (r != NULL && r->primero != NULL && r->disponible >= r->primero->cantidad && *cant_notificaciones < cant_max) {
            unsigned int id_pendiente = r->primero->id;
            int socket_pendiente = r->primero->socket;
            unsigned int cantidad_pendiente = r->primero->cantidad;
            desencolar(r);

            r->disponible -= cantidad_pendiente;
            hash_insertar(rm->activos, id_pendiente, socket_pendiente, j, cantidad_pendiente);

            notificaciones[*cant_notificaciones].socket = socket_pendiente;
            notificaciones[*cant_notificaciones].job_id = id_pendiente;
            notificaciones[*cant_notificaciones].recurso = j; // j representa el índice real del recurso
            notificaciones[*cant_notificaciones].cantidad = cantidad_pendiente;
            (*cant_notificaciones)++;
        }
    }
    
    return 0;
}

// limpiar_cola: funcion auxiliar encargada de liberar los nodos de la cola de un recurso que coincidan con un socket.
void limpiar_cola(recurso r, int socket){
    if (r == NULL || r->primero == NULL) return;

    struct job_pendiente *actual = r->primero;
    struct job_pendiente *anterior = NULL;

    while (actual != NULL) {
        if (actual->socket == socket) {
            struct job_pendiente * a_borrar = actual;

            if (anterior == NULL) {
                r->primero = actual->siguiente;
                actual = r->primero;
            } else {
                anterior->siguiente = actual->siguiente;
                actual = actual->siguiente;
            }

            if (a_borrar == r->ultimo) {
                r->ultimo = anterior;
            }

            free(a_borrar);
        } else {
            anterior = actual;
            actual = actual->siguiente;
        }
    }
}

// remover_de_cola_pendiente_por_id: Quita un job de la cola FIFO usando su id
// Se usa como rollback inmediato si una reserva distribuida falla en otro nodo
void remover_de_cola_pendiente_por_id(recurso r, unsigned int id) {
    if (r == NULL || r->primero == NULL) return;
    
    struct job_pendiente *anterior = NULL;
    struct job_pendiente *actual = r->primero;
    
    while (actual != NULL) {
        if (actual->id == id) {
            struct job_pendiente *a_borrar = actual;
            if (anterior == NULL) {
                r->primero = actual->siguiente;
            } else {
                anterior->siguiente = actual->siguiente;
            }
            if (actual == r->ultimo) {
                r->ultimo = anterior;
            }
            actual = actual->siguiente;
            free(a_borrar);
            printf("[INFO] Rollback: Removido Job %u de la cola de pendientes.\n", id);
            return; 
        } else {
            anterior = actual;
            actual = actual->siguiente;
        }
    }
}

// handler_disconnect: funcion principal encargada de manejar una desconexion repentina de un cliente.
// libera tanto los trabajos activos como los pendientes asociados a un socket.
void handler_disconnect(ResourceManager * rm, int socket, Notificacion* notificaciones, int* cant_notificaciones, int cant_max){
    unsigned int tam = rm->activos->capacidad;

    for (unsigned int i = 0; i < tam; i++){
        struct job_activo *actual = rm->activos->tabla[i];
        struct job_activo *anterior = NULL;

        while (actual != NULL){
            if (actual->socket == socket) {
    
                rm->cpu->disponible += actual->cpu_asignado;
                rm->gpu->disponible += actual->gpu_asignado;
                rm->mem->disponible += actual->mem_asignado;
                
                struct job_activo *a_borrar = actual;
                
                if (anterior == NULL) {
                    rm->activos->tabla[i] = actual->siguiente;
                    actual = actual->siguiente;
                } else {
                    anterior->siguiente = actual->siguiente;
                    actual = actual->siguiente;
                }

                free(a_borrar);
            }
            else {
                anterior = actual;
                actual = actual->siguiente;
            }
        }
    }

    limpiar_cola(rm->cpu, socket);
    limpiar_cola(rm->gpu, socket);
    limpiar_cola(rm->mem, socket);

    recurso recursos_a_revisar[3] = {rm->cpu, rm->gpu, rm->mem};

    for (int j = 0; j < 3; j++) {
        recurso r = recursos_a_revisar[j];
        while (r != NULL && r->primero != NULL && r->disponible >= r->primero->cantidad && *cant_notificaciones < cant_max) {

            unsigned int id_pendiente = r->primero->id;
            int socket_pendiente = r->primero->socket;
            unsigned int cantidad_pendiente = r->primero->cantidad;

            desencolar(r);

            r->disponible -= cantidad_pendiente;
            hash_insertar(rm->activos, id_pendiente, socket_pendiente, j, cantidad_pendiente);

            notificaciones[*cant_notificaciones].socket = socket_pendiente;
            notificaciones[*cant_notificaciones].job_id = id_pendiente;
            notificaciones[*cant_notificaciones].recurso = j; // j representa el índice real del recurso
            notificaciones[*cant_notificaciones].cantidad = cantidad_pendiente;
            (*cant_notificaciones)++;
        }
    }
}