# Middleware Distribuido de Gestión de Recursos Concurrentes

Este proyecto consiste en un middleware distribuido, heterogéneo e interoperable diseñado para coordinar la asignación y liberación de recursos de hardware (CPU, memoria y GPU) en un clúster de nodos de cómputo.

El sistema implementa una arquitectura híbrida que combina la coordinación global basada en actores de **Erlang** con la eficiencia de bajo nivel y el procesamiento dirigido por eventos de **Lenguaje C**.

---

## Tecnologías Utilizadas

* Erlang/OTP
* Lenguaje C
* TCP
* UDP Broadcast
* epoll (Linux)
* Bash
* Make

---

## Arquitectura General

> El diagrama completo de arquitectura y secuencia puede consultarse en el informe técnico incluido en `/docs`.

```text
Scheduler (Erlang)
        |
        | TCP
        v
Agente C Local <-------> Agentes C Remotos
        ^
        |
    UDP Broadcast
```

---

## Arquitectura del Sistema

El middleware se divide en dos componentes principales que se comunican mediante sockets TCP, permitiendo tanto el control local del planificador como la comunicación con agentes remotos.

### 1. Planificador Global (Erlang)

Centraliza las solicitudes de los Jobs mediante un modelo basado en actores, administra el ciclo de vida de los procesos concurrentes y previene bloqueos mutuos mediante una política de Orden Total sobre la adquisición de recursos.

### 2. Agente de Concurrencia Local (C)

Se ejecuta en cada nodo físico utilizando multiplexación de entrada/salida no bloqueante mediante `epoll`.

Sus responsabilidades principales son:

* Administrar los recursos locales mediante el `ResourceManager`.
* Procesar solicitudes TCP locales y remotas.
* Descubrir automáticamente nodos vecinos mediante UDP Broadcast.
* Mantener una tabla dinámica de nodos disponibles en la red.

---

## Estrategia Anti-Deadlock (Prevención por Orden Total)

Para evitar la condición de espera circular de Coffman, el planificador de Erlang impone un **Orden Total** estricto sobre todos los recursos solicitados antes de despachar un comando `JOB_REQUEST`.

### Nivel 1: Nodo de Destino

Los recursos se ordenan utilizando el par:

```text
{IP, PuertoPublico}
```

Esto garantiza una jerarquía global única incluso cuando varios nodos comparten la misma máquina física durante las pruebas.

### Nivel 2: Tipo de Recurso

Cuando varios recursos pertenecen al mismo nodo, se aplica la siguiente prioridad fija:

```text
cpu → mem → gpu
```

De esta forma todos los procesos solicitan recursos siguiendo exactamente el mismo orden, eliminando la posibilidad de espera circular dentro del modelo implementado.

---

## Requisitos Previos

Asegúrese de disponer de:

* Sistema operativo Linux.
* Compilador `gcc`.
* Herramienta `make`.
* Erlang/OTP 25 o superior.

> **Nota:** El proyecto fue desarrollado y probado sobre Linux debido al uso de la API `epoll`.

---

## Compilación

Desde la raíz del repositorio:

```bash
make
```

Esto generará:

* El ejecutable nativo `./agent`.
* Los archivos compilados de Erlang (`.beam`) dentro de `erlang_scheduler/`.

---

## Ejecución Rápida

Para compilar el proyecto y ejecutar el escenario principal de prueba:

```bash
make
chmod +x scripts/test_deadlock.sh
./scripts/test_deadlock.sh
```

---

## Ejecución Manual

### 1. Levantar un Agente (C)

```bash
./agent <puerto_publico> <puerto_local> <cant_cpu> <cant_memoria> <cant_gpu>
```

Donde:

* `puerto_publico`: puerto utilizado para recibir solicitudes remotas.
* `puerto_local`: puerto utilizado por el scheduler de Erlang.
* `cant_cpu`: cantidad de CPUs disponibles.
* `cant_memoria`: cantidad de memoria disponible.
* `cant_gpu`: cantidad de GPUs disponibles.

Ejemplo:

```bash
./agent 8100 9100 2 8 0
```

### 2. Iniciar el Scheduler (Erlang)

En otra terminal:

```bash
erl -noshell -pa erlang_scheduler -eval "scheduler:iniciar(9100)." -s init stop
```

---

## Pruebas Automatizadas

El proyecto incluye scripts de prueba para reproducir distintos escenarios de concurrencia y contención de recursos.

### Escenario A: Contención y Prevención de Deadlock

```bash
chmod +x scripts/test_deadlock.sh
./scripts/test_deadlock.sh
```

Este escenario reproduce solicitudes concurrentes con recursos limitados.

**Resultado esperado:**

* Se observará contención sobre los recursos.
* El sistema resolverá el conflicto sin producir interbloqueos.
* Las solicitudes serán concedidas o rechazadas según la disponibilidad de recursos.

---

### Escenario B: Flujo Concurrente Completo (Sin Escasez)

```bash
chmod +x scripts/test_sin_deadlock.sh
./scripts/test_sin_deadlock.sh
```

Este escenario asigna recursos suficientes para satisfacer todas las solicitudes concurrentes.

**Resultado esperado:**

* Todos los Jobs finalizan exitosamente.
* Las respuestas retornan como `JOB_GRANTED`.

---

## Estructura del Repositorio

```text
.
├── c_agent/
│   ├── main.c
│   ├── server.c
│   ├── resource_manager.c
│   └── ...
│
├── erlang_scheduler/
│   ├── scheduler.erl
│   ├── simulador.erl
│   ├── cliente_tcp.erl
│   └── ...
│
├── scripts/
│   ├── test_deadlock.sh
│   └── test_sin_deadlock.sh
│
├── docs/
│   └── informe.pdf
│
└── Makefile
```

### Descripción de Carpetas

* **c_agent/**: implementación del servidor multipuerto en C, lógica de red basada en `epoll` y administración de recursos.
* **erlang_scheduler/**: módulos de planificación, comunicación TCP y prevención de deadlocks mediante Orden Total.
* **scripts/**: automatización de escenarios de prueba.
* **docs/**: documentación técnica del proyecto.
* **Makefile**: compilación unificada de todos los componentes.

---

## Autores

* Agustín Messana Gullielmi
* Nahuel Deppen
* Felipe Pineda
* Gaspar Emilio Rubies

Trabajo Práctico Final — Sistemas Operativos.
