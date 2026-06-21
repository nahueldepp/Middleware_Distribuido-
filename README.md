# Middleware Distribuido de Gestión de Recursos Concurrentes

Este proyecto consiste en un middleware distribuido, heterogéneo e interoperable diseñado para coordinar la asignación y liberación de recursos de hardware (CPU, memoria y GPU) en un clúster de nodos de cómputo en tiempo real. 

El sistema implementa una arquitectura híbrida que combina la robustez en la coordinación global de **Erlang** con la eficiencia de bajo nivel y el procesamiento dirigido por eventos de **Lenguaje C**.

---

## Arquitectura del Sistema

El middleware se divide en dos componentes principales que se comunican mediante sockets TCP locales a través del protocolo textual estandarizado en la sección **§5.3**:

1. **Planificador Global (Erlang):** Centraliza las solicitudes de los Jobs a través de un lazo asincrónico basado en actores, gestiona el ciclo de vida de los procesos concurrentes y previene activamente situaciones de bloqueo mutuo (*deadlocks*).
2. **Agente de Concurrencia Local (C):** Se ejecuta en cada nodo físico multiplexando la entrada/salida de forma no bloqueante mediante `epoll`. Administra el stock local a través del `ResourceManager` y descubre dinámicamente a sus pares mediante ráfagas periódicas de UDP Broadcast.

### Estrategia Anti-Deadlock
Para romper la condición de *espera circular* de Coffman, el planificador de Erlang impone un **Orden Total** estricto en la adquisición de recursos antes de despachar cualquier comando `JOB_REQUEST` hacia la capa en C:
* **Nivel 1:** Ordenamiento jerárquico lexicográfico por dirección IP del nodo destino.
* **Nivel 2:** Ordenamiento de prioridad fijo por tipo de recurso físico (`cpu` $\rightarrow$ `mem` $\rightarrow$ `gpu`).

---

## Requisitos Previos

Asegúrese de contar con las siguientes herramientas instaladas en su entorno Linux:
* Compilador `gcc` (soporte para llamadas del sistema POSIX y `epoll`).
* Entorno de ejecución de Erlang (OTP 25 o superior / `erl`).
* Utilidad `netcat` (`nc`) para simulaciones de red aisladas.

---

## Guía de Ejecución Rápida

Siga este orden secuencial para iniciar el entorno distribuido en su máquina local:

### 1. Compilar y Levantar el Agente en C
Navegue hasta el directorio del agente, compile el binario nativo y ejecútelo especificando el **puerto público** (comunicación entre nodos) y el **puerto local** (interfaz de Erlang):

```bash
gcc -Wall -Wextra -g c_agent/server.c -o agent
./agent 8100 9100

```

### 2. Levantar el Planificador de Erlang

En una nueva terminal (en la raíz del proyecto), compile los módulos lógicos e inicialice el componente del planificador indicando el puerto de la interfaz local del agente en C (`9100`):

```bash
erlc cliente_tcp.erl scheduler.erl
erl -noshell -s scheduler iniciar 9100 -s init stop

```

Al iniciar, el planificador se conectará de forma automática al puerto TCP local del agente, le enviará el comando `GET NODES` y spawneará de forma interna el proceso del simulador para comenzar a procesar Jobs de forma distribuida.

---

## Pruebas, Simulación y Logs

### Formato de Comunicación del Protocolo

Cuando el sistema corre de forma integrada, la interacción textual sigue la especificación de la cátedra mediante los siguientes comandos automáticos:

* `JOB_REQUEST <IdJob> @<IP>:<Recurso>:<Cantidad>`: Solicitud ordenada secuencialmente enviada por Erlang.
* `JOB_GRANTED <IdJob>` / `JOB_DENIED <IdJob>`: Veredicto síncrono retornado por el agente en C.
* `JOB_RELEASE <IdJob>`: Directiva de liberación mandatoria despachada por Erlang tras cumplirse un tiempo de uso fijo de 5 segundos (`5000ms`).

### Monitoreo del Clúster

Toda la actividad del middleware (la conexión con C, el parseo de los recursos del hardware, la generación de identificadores únicos de Jobs y las resoluciones de asignación) queda registrada de forma permanente. Puede monitorear la bitácora del clúster en tiempo real ejecutando:

```bash
tail -f scheduler.log

```

---

## Estructura del Repositorio

* `/c_agent`: Código fuente del servidor multipuerto en C, lógica de red basada en `epoll` y descubrimiento UDP broadcast.
* `/`: Módulos de orquestación, planificador de red y prevención de deadlocks en Erlang (`scheduler.erl`, `cliente_tcp.erl`).