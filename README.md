# Middleware Distribuido de Gestión de Recursos Concurrentes

Este proyecto consiste en un middleware distribuido, heterogéneo e interoperable diseñado para coordinar la asignación y liberación de recursos de hardware (CPU, memoria y GPU) en un clúster de nodos de cómputo en tiempo real. 

El sistema implementa una arquitectura híbrida que combina la robustez en la coordinación global y el modelo de actores de **Erlang** con la eficiencia de bajo nivel y el procesamiento dirigido por eventos de **Lenguaje C**.

---

## Arquitectura del Sistema

El middleware se divide en dos componentes principales que se comunican mediante sockets TCP locales a través del protocolo de la interfaz local estandarizado en las secciones **§4.1** y **§4.2** del enunciado:

1. **Planificador Global (Erlang):** Centraliza las solicitudes de los Jobs a través de un lazo asincrónico basado en actores, gestiona el ciclo de vida de los procesos concurrentes y previene activamente situaciones de bloqueo mutuo (*deadlocks*).
2. **Agente de Concurrencia Local (C):** Se ejecuta en cada nodo físico multiplexando la entrada/salida de forma no bloqueante mediante `epoll`. Administra el stock local a través del `ResourceManager` y descubre dinámicamente a sus pares mediante ráfagas periódicas de UDP Broadcast.

### Estrategia Anti-Deadlock (Prevención por Orden Total)
Para romper la condición de *espera circular* de Coffman en el entorno distribuido, el planificador de Erlang impone un **Orden Total** estricto en la adquisición de recursos antes de despachar cualquier comando `JOB_REQUEST` hacia la capa en C:
* **Nivel 1:** Ordenamiento jerárquico global por el par **`{IP, PuertoLocal}`** del nodo destino (Opción A). Esto evita la ambigüedad cuando se simulan múltiples nodos dentro de una misma máquina de testeo.
* **Nivel 2:** Ordenamiento de prioridad interno fijo por tipo de recurso físico (`cpu` $\rightarrow$ `mem` $\rightarrow$ `gpu`).

---

## Requisitos Previos

Asegúrese de contar con las siguientes herramientas instaladas en su entorno Linux:
* Compilador `gcc` y herramienta `make` (soporte para llamadas del sistema POSIX y `epoll`).
* Entorno de ejecución de Erlang (OTP 25 o superior / `erlc` y `erl`).

---

## Guía de Compilación y Ejecución

El proyecto incluye un `Makefile` unificado en la raíz que compila de forma simultánea el binario en C y los archivos lógicos de Erlang.

### 1. Compilación Unificada
Ejecute el siguiente comando en la raíz del repositorio:
```bash
make

```

Esto generará el ejecutable nativo `./agent` y colocará los archivos binarios de Erlang (`.beam`) dentro del directorio `/erlang_scheduler`.

### 2. Ejecución Manual de un Agente (C)

Para levantar un nodo individual, ejecute el binario especificando los 5 parámetros obligatorios de inicialización:

```bash
./agent <puerto_publico> <puerto_local> <cant_cpu> <cant_memoria> <cant_gpu>

```

*Ejemplo para el Nodo A:*

```bash
./agent 8100 9100 2 8 0

```

### 3. Ejecución Manual del Planificador (Erlang)

En una nueva terminal, inicialice el componente del planificador indicando el puerto de la interfaz local del agente en C al que desea conectarse (ej. `9100`):

```bash
erl -noshell -pa erlang_scheduler -eval "scheduler:iniciar(9100)." -s init stop

```

---

## Pruebas Automatizadas (Escenarios del TP)

Para facilitar la evaluación y la demo en vivo, el repositorio cuenta con dos scripts de automatización dentro de la carpeta `/scripts` que recrean las condiciones de contención distribuidas descritas en la sección **§6**:

### Escenario A: Contención y Prevención de Deadlock

Simula el solapamiento temporal exacto del enunciado (Job1 y Job2 cruzados) bajo condiciones de escasez. Demuestra cómo el sistema resuelve la contención de hardware de forma segura mediante un rechazo controlado sin colgarse jamás:

```bash
chmod +x scripts/test_deadlock.sh
./scripts/test_deadlock.sh

```

*Resultado esperado:* Un Job terminará en `JOB_GRANTED` y el otro en `JOB_DENIED` debido al Orden Total estricto.

### Escenario B: Flujo Concurrente Completo (Sin Escasez)

Simula el mismo entorno cruzado pero otorgándole hardware de sobra a los nodos para comprobar que ambos Jobs se conceden limpios en paralelo si la red posee la capacidad suficiente:

```bash
chmod +x scripts/test_sin_deadlock.sh
./scripts/test_sin_deadlock.sh

```

*Resultado esperado:* Ambos Jobs finalizan exitosamente en `JOB_GRANTED`.

---

## Estructura del Repositorio

* `/c_agent`: Código fuente del servidor multipuerto en C (`main.c`, `server.c`), lógica de red basada en `epoll` y estructuras del administrador de pozo (`resource_manager.c`).


* `/erlang_scheduler`: Módulos de orquestación asincrónica, lógica de la interfaz cliente TCP y ordenamiento preventivo anti-deadlocks en Erlang (`scheduler.erl`, `simulador.erl`, `cliente_tcp.erl`).


* `/scripts`: Scripts Bash automatizados para el despliegue y control de las simulaciones locales de interbloqueo.


* `/Makefile`: Directivas del sistema de automatización de compilación unificada.