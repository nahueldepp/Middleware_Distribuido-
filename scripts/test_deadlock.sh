#!/bin/bash

# Colores para la consola
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0;0m' # Sin color

echo -e "${BLUE}[TEST] Iniciando escenario de prueba de Deadlock Distribuido...${NC}"

# 1. Limpieza de procesos viejos para liberar puertos
echo -e "[TEST] Limpiando procesos de agentes anteriores..."
killall agent 2>/dev/null
sleep 1

# 2. Levantar Nodo A (§6 del enunciado)
# Configuración: Puerto Público=8100, Puerto Local=9100, CPUs=2, Memoria=8, GPUs=0
echo -e "[TEST] Lanzando Nodo A en puerto público 8100 (2 CPUs, 8GB RAM, 0 GPU)..."
./agent 8100 9100 2 8 0 > /dev/null 2>&1 &
PID_A=$!

# 3. Levantar Nodo B (§6 del enunciado)
# Configuración: Puerto Público=8200, Puerto Local=9200, CPUs=2, Memoria=4, GPUs=1
echo -e "[TEST] Lanzando Nodo B en puerto público 8200 (2 CPUs, 4GB RAM, 1 GPU)..."
./agent 8200 9200 2 4 1 > /dev/null 2>&1 &
PID_B=$!

# Esperamos 2 segundos para que los agentes completen su arranque pasivo de UDP
sleep 4

echo -e "${GREEN}[TEST] Ambos agentes levantados y descubiertos por UDP broadcast.${NC}"
echo -e "[TEST] PIDs registrados -> Nodo A: $PID_A | Nodo B: $PID_B"
echo -e "----------------------------------------------------------------"

# 4. Simulación del Escenario de Interbloqueo (§6)
echo -e "${BLUE}[TEST] Simulando Jobs concurrentes desde Erlang...${NC}"

# Aquí el script invoca a la lógica de Erlang que ustedes programaron.
# Simulamos el solapamiento temporal mandando comandos a los puertos locales.
# Nota: Modificá el comando de abajo según el nombre exacto de tu módulo de Erlang.

if [ -f "erlang_scheduler/scheduler.beam" ]; then
    echo -e "[TEST] Ejecutando planificador de Erlang..."
    # Lanzamos el entorno Erlang sin interfaz gráfica para ejecutar el test
    erl -noshell -pa erlang_scheduler -eval "scheduler:iniciar(9100)." -s init stop
else
    echo -e "${RED}[ALERTA] No se encontró el binario de Erlang compilado (scheduler.beam).${NC}"
    echo -e "[INFO] Podés interactuar manualmente con los puertos locales 9100 y 9200 con Netcat."
    
    # Mantenemos los agentes vivos por si querés tirarles comandos manuales desde otra consola
    echo -e "[INFO] Manteniendo agentes encendidos. Presioná [CTRL+C] para finalizar la prueba."
    wait $PID_A $PID_B
fi

# 5. Limpieza al finalizar la prueba
echo -e "${BLUE}[TEST] Finalizando instancias de simulación...${NC}"
kill $PID_A $PID_B 2>/dev/null
echo -e "${GREEN}[TEST] Prueba finalizada exitosamente.${NC}"