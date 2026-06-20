#include <stdio.h>
#include <stdlib.h>

#include "resource_manager.h"
#include "server.h"

int main(int argc, char **argv) {
    if (argc != 6) {
        fprintf(stderr, "Uso: %s <puerto_publico> <puerto_local> <cpu> <mem> <gpu>\n", argv[0]);
        return 1;
    }

    int puerto_publico = atoi(argv[1]);
    int puerto_local = atoi(argv[2]);
    int cpu = atoi(argv[3]);
    int mem = atoi(argv[4]);
    int gpu = atoi(argv[5]);

    ResourceManager rm;
    resources_init(&rm, cpu, gpu, mem, 128);

    printf("[INFO] Agent starting\n");
    printf("[INFO] Public port: %d\n", puerto_publico);
    printf("[INFO] Local port: %d\n", puerto_local);
    printf("[INFO] Resources: cpu=%d mem=%d gpu=%d\n", cpu, mem, gpu);

    server_run(puerto_publico, puerto_local, &rm);

    return 0;
}