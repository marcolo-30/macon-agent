# Anatomía del `cluster_agent.py`

Este documento describe la arquitectura y el flujo de trabajo del `cluster_agent.py`, un agente autónomo basado en el ciclo de control **MAPE-K** (Monitor, Analyze, Plan, Execute over a Knowledge base) y construido sobre la plataforma de agentes **SPADE**.

El objetivo principal del agente es monitorizar el estado de una aplicación (`main-app`) en un clúster de Kubernetes y migrarla entre nodos para optimizar su rendimiento o escapar de un entorno degradado.

---

## 1. Estado y Memoria del Agente (La "K" de MAPE-K)

La memoria del agente reside en las variables de instancia de la clase `ClusterAgent`. La más importante es `self.knowledge`, una base de conocimiento compartida por todos los comportamientos.

### `ClusterAgent.setup()`
Aquí se inicializa el estado del agente:

- **`self.knowledge` (dict)**: Un diccionario que funciona como la **memoria a corto plazo** del agente. Contiene las métricas más recientes del entorno.
  - `knowledge['nodes_cpu']`: Un diccionario con el uso de CPU de cada nodo del clúster (e.g., `{'r3-node': 85.1, 'vm1node': 44.2}`).
  - `knowledge['qos']`: El valor numérico de la métrica `main_app_qos`.
  - `knowledge['target_node']`: El nodo al que se ha decidido migrar. Lo escribe el `AnalyzeBehaviour` y lo lee el `PlanBehaviour`.

- **`self.current_node` (str)**: La **creencia** del agente sobre en qué nodo se encuentra actualmente la aplicación. Se inicializa a `"unknown"` y el `MonitorBehaviour` se encarga de verificar y corregir esta creencia en cada ciclo.

- **`self.migration_in_flight` (bool)**: Una bandera o "lock" crucial. Se pone a `True` cuando se inicia una migración y a `False` cuando termina. Evita que el agente intente iniciar una nueva migración mientras otra ya está en curso.

- **Parámetros de Configuración y Política**:
  - `prometheus_url`, `namespace`, `deployment_to_migrate`, etc.
  - `qos_threshold`, `return_cpu_threshold`: Los umbrales que definen la política de decisión del agente.

---

## 2. Los Comportamientos (`Behaviours`)

El ciclo MAPE se implementa a través de una serie de `behaviours` de SPADE que se comunican a través de la memoria del agente.

### `MonitorBehaviour` (Monitor)
- **Tipo**: `PeriodicBehaviour` (se ejecuta cada 15 segundos).
- **Objetivo**: **Percibir el estado del mundo**. Es los "sentidos" del agente.
- **¿Qué hace?**:
  1.  **Propiocepción (Saber dónde está)**: Llama a la API de Kubernetes para obtener la ubicación **real** del pod `main-app` y actualiza `self.current_node`. Este es el paso más importante para la robustez del agente.
  2.  **Observación Externa**: Lanza consultas concurrentes a la API de Prometheus para obtener las métricas de CPU de los nodos y la QoS de la aplicación.
- **Información que produce**: Actualiza `self.knowledge['nodes_cpu']` y `self.knowledge['qos']`.
- **Ejecuciones externas**:
  - `kubectl` (a través de la librería de Python) para listar pods.
  - Peticiones HTTP a la API de Prometheus.

### `AnalyzeBehaviour` (Analyze)
- **Tipo**: `PeriodicBehaviour` (se ejecuta cada 15 segundos).
- **Objetivo**: **Decidir si se debe actuar**. Es el "cerebro" del agente.
- **¿Qué hace?**:
  1.  Lee el estado actual del mundo desde `self.knowledge` y `self.current_node`.
  2.  Aplica la política de decisión definida:
      - **Si está en `r3-node`**: Comprueba si la QoS ha caído por debajo del `qos_threshold`.
      - **Si está en `vm1node`**: Comprueba si la CPU de `r3-node` ha bajado del `return_cpu_threshold`.
  3.  Si alguna de las condiciones se cumple, decide iniciar una migración.
- **Información que produce**: Si decide migrar, escribe el nodo de destino en `self.knowledge['target_node']` y activa el `PlanBehaviour`. También pone el "lock" `self.migration_in_flight` a `True`.
- **Ejecuciones externas**: Ninguna. Es un proceso de lógica puramente interno.

### `PlanBehaviour` (Plan)
- **Tipo**: `OneShotBehaviour` (se ejecuta una sola vez cuando es llamado).
- **Objetivo**: **Preparar la ejecución de la acción**. En nuestra arquitectura actual, es un paso de transición muy simple.
- **¿Qué hace?**:
  1.  Lee el `target_node` desde `self.knowledge`.
  2.  Activa inmediatamente el `ExecuteBehaviour`.
- **Información que produce**: Ninguna. Actúa como un simple intermediario.
- **Ejecuciones externas**: Ninguna.

### `ExecuteBehaviour` (Execute)
- **Tipo**: `OneShotBehaviour`.
- **Objetivo**: **Actuar sobre el entorno**. Son los "brazos" del agente.
- **¿Qué hace?**:
  1.  Lee el `target_node` desde `self.knowledge`.
  2.  Construye y ejecuta el comando `kubectl patch deployment ...` para cambiar el `nodeSelector` del `Deployment` de `main-app`. Esta es la acción que provoca la migración.
- **Información que produce**: Al finalizar (con éxito o fracaso), resetea el "lock" `self.migration_in_flight` a `False`, permitiendo que el `AnalyzeBehaviour` pueda tomar nuevas decisiones.
- **Ejecuciones externas**:
  - `kubectl` (a través de la librería de Python) para modificar un recurso del clúster.

---

## 3. Flujo de Información y Control

El flujo es un ciclo continuo orquestado por SPADE:

1.  El **`MonitorBehaviour`** se ejecuta periódicamente, actualizando la memoria (`self.knowledge`) con datos frescos del clúster.
2.  El **`AnalyzeBehaviour`** se ejecuta periódicamente, leyendo esa memoria.
3.  Si el `AnalyzeBehaviour` decide actuar, **pasa el control** añadiendo el **`PlanBehaviour`** a la cola de ejecución del agente.
4.  El **`PlanBehaviour`** se ejecuta una vez e inmediatamente **pasa el control** al **`ExecuteBehaviour`**.
5.  El **`ExecuteBehaviour`** realiza la acción externa y, al terminar, libera el "lock" `migration_in_flight`.
6.  El ciclo vuelve a empezar, con el `Monitor` y el `Analyze` ejecutándose periódicamente hasta que se cumpla la siguiente condición para actuar.
