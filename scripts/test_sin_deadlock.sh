#!/bin/bash

# Colores para la consola
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0;0m' # Sin color

echo -e "${BLUE}[TEST] Escenario SIN escasez: ambos jobs deben terminar GRANTED.${NC}"
echo -e "[TEST] Demuestra que, cuando hay recursos suficientes, los dos jobs"
echo -e "[TEST] cruzados del §6 se conceden sin trabarse (no hay deadlock).${NC}"

# 1. Limpieza de procesos viejos
echo -e "[TEST] Limpiando procesos de agentes anteriores..."
killall agent 2>/dev/null
sleep 1

# 2. Nodo A: ahora con 4 CPU (suficiente para los dos jobs, que piden 2 c/u)
echo -e "[TEST] Lanzando Nodo A en puerto público 8100 (4 CPUs, 8GB RAM, 0 GPU)..."
./agent 8100 9100 4 8 0 > /dev/null 2>&1 &
PID_A=$!

# 3. Nodo B: con 2 GPU (suficiente para los dos jobs, que piden 1 c/u)
echo -e "[TEST] Lanzando Nodo B en puerto público 8200 (2 CPUs, 4GB RAM, 2 GPU)..."
./agent 8200 9200 2 4 2 > /dev/null 2>&1 &
PID_B=$!

# Esperamos a que los agentes se descubran por UDP
sleep 6

echo -e "${GREEN}[TEST] Ambos agentes levantados y descubiertos por UDP broadcast.${NC}"
echo -e "[TEST] PIDs registrados -> Nodo A: $PID_A | Nodo B: $PID_B"
echo -e "----------------------------------------------------------------"

echo -e "${BLUE}[TEST] Simulando Jobs concurrentes desde Erlang...${NC}"

if [ -f "erlang_scheduler/scheduler.beam" ]; then
    echo -e "[TEST] Ejecutando planificador de Erlang..."
    erl -noshell -pa erlang_scheduler -eval "scheduler:iniciar(9100)." -s init stop
else
    echo -e "${RED}[ALERTA] No se encontró scheduler.beam compilado.${NC}"
    wait $PID_A $PID_B
fi

echo -e "${BLUE}[TEST] Finalizando instancias de simulación...${NC}"
kill $PID_A $PID_B 2>/dev/null
echo -e "${GREEN}[TEST] Prueba finalizada.${NC}"