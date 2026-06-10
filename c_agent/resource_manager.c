#include <stdio.h>
#include <stdlib.h>

struct job {
    int id;
    // socket
    int resource_solicitado; // cpu = 0, gpu = 1, mem = 2
};

typedef struct Node {
    int valor;
    struct SNode * next; 
} * SNode;

typedef struct res {
    int total;
    int disponible;
    SNode primero;
    SNode ultimo;
} * resource;

typedef struct resman {
    resource cpu;
    resource gpu;
    resource mem;
} ResourceManager;

void resources_init(ResourceManager * rm, int cant_cpu, int cant_gpu, int cant_mem){
    rm->cpu = malloc(sizeof(struct res));
    rm->gpu = malloc(sizeof(struct res));
    rm->mem = malloc(sizeof(struct res));

    rm->cpu->total = rm->cpu->disponible = cant_cpu;
    rm->gpu->total = rm->gpu->disponible = cant_gpu;
    rm->mem->total = rm->mem->disponible = cant_mem;

    rm->cpu->primero = rm->cpu->ultimo = NULL;
    rm->gpu->primero = rm->gpu->ultimo = NULL;
    rm->mem->primero = rm->mem->ultimo = NULL;
}