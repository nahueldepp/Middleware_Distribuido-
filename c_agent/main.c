#include <stdio.h>
#include <stdlib.h>
#include "resources.h"
#include "server.h"

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "Uso: %s <puerto> <cpu> <mem> <gpu>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    int cpu = atoi(argv[2]);
    int mem = atoi(argv[3]);
    int gpu = atoi(argv[4]);

    ResourceManager rm;
    resources_init(&rm);
    resources_add(&rm, "cpu", cpu);
    resources_add(&rm, "mem", mem);
    resources_add(&rm, "gpu", gpu);

    printf("[INFO] Agent starting on port %d\n", port);
    printf("[INFO] Resources: cpu=%d mem=%d gpu=%d\n", cpu, mem, gpu);

    server_run(port, &rm);

    return 0;
}