# Metodología y Estructura del Artículo: Early Warning Signals for Bus Bunching
*Target Journal: ETASR (Q2 Engineering)*

## 1. Introducción
- **Problema**: Degradación del servicio por agrupación de vehículos (Bus Bunching).
- **Limitación**: El GTFS estático es insuficiente para la gestión proactiva; necesidad de arquitecturas de Gemelo Digital basadas en GTFS-RT.
- **Contribución**: Identificación empírica de señales de alerta temprana mediante el análisis de la derivada del retraso (Delay Drift) en corredores de alta frecuencia.

## 2. Marco Analítico y Formulación Matemática
### 2.1. Cálculo de Headway (H)
Se define el headway observado ($h_{obs}$) y programado ($h_{sch}$) en la parada $i$ para el vehículo $v_2$ respecto a su predecesor $v_1$:
$$h_{obs}(i) = T_{obs}(i, v_2) - T_{obs}(i, v_1)$$
$$h_{sch}(i) = T_{sch}(i, v_2) - T_{sch}(i, v_1)$$

### 2.2. Definición de Anomalía (Ground Truth)
Un evento se etiqueta como *Bunching* ($B=1$) si:
$$h_{obs} < 0.25 \cdot h_{sch}$$

### 2.3. Señales de Alerta Temprana (Early Warning)
Se analiza la evolución del Delay Drift ($D = T_{obs} - T_{sch}$) en las paradas inmediatamente anteriores ($n-1, n-2, n-3$) al evento $B_n=1$. El objetivo es determinar si $\Delta D/\Delta n$ (la pendiente del retraso) es un predictor estadísticamente superior a la caída del headway temporal.

## 3. Configuración Experimental
- **Dataset**: AVL Data (511.org) de la ruta SF:14 (Mission St).
- **Volumen**: ~1M de registros (Febrero 2026).
- **Limpieza**: Filtrado de outliers de GPS y normalización de tiempos > 24:00.

## 4. Resultados y Discusión
- **Tracking Secuencial**: Evolución de $D$ vs $H$ en el espacio-tiempo previo a la anomalía.
- **Evaluación de Rendimiento**: Matriz de confusión (TPR/FPR) y tiempo de antelación (Lead Time).
- **Implicaciones**: Requerimientos de latencia para sistemas de gemelos digitales en tiempo real.

## 5. Resultados Preliminares: Evaluación del Clasificador de Alerta Temprana

Se evaluó un clasificador de referencia (Baseline) utilizando un umbral de crecimiento de retraso ($\Delta D > 60$ s) en la parada $n-1$. Los resultados demuestran la insuficiencia de métricas aisladas para la predicción de estados críticos:

| Métrica | Valor | Interpretación |
| :--- | :--- | :--- |
| **TPR (True Positive Rate)** | 0.2239 | Solo se captura el 22% de los eventos de bunching de forma anticipada. |
| **FPR (False Positive Rate)** | 0.3867 | Elevada tasa de falsas alarmas (~39%), indicando ruido sistémico. |

### 5.1. Matriz de Confusión (Baseline)
- **Verdaderos Positivos (TP)**: 17,746
- **Falsos Positivos (FP)**: 95,951
- **Falsos Negativos (FN)**: 61,528
- **Verdaderos Negativos (TN)**: 152,162

### 5.2. Discusión de los Hallazgos
La alta tasa de falsos positivos sugiere que los incrementos puntuales en el retraso ($D$) no son precursores deterministas del agrupamiento de vehículos ($B$). Factores exógenos (semáforos, demanda aleatoria en paradas) introducen varianza en $D$ que no necesariamente colapsa el headway ($H$). 

**Justificación de la Propuesta**: Estos resultados validan la necesidad de un análisis de **ventana secuencial (n-3)** que observe la tendencia (pendiente y aceleración) tanto de $D$ como de $H$ para detectar el colapso inminente del intervalo.

### 5.5. Resultados de la Predicción de Transición y Generalización Espacial
Para validar la robustez del modelo en un escenario de **Gemelo Digital**, se realizó una evaluación de "Predicción de Transición" bajo aislamiento temporal (21/7 días) y espacial (Transfer Learning). El modelo entrenado exclusivamente con la Ruta 14 (Mission St) fue evaluado en corredores independientes (Ruta 38 Geary Blvd y Ruta 49 Van Ness Ave).

| Métrica | Ruta 14 (Temporal) | Ruta 38 (Spatial) | Ruta 49 (Spatial) |
| :--- | :--- | :--- | :--- |
| **F1-Score (Máximo)** | 0.8227 | 0.8549 | 0.8576 |
| **Precision-Recall AUC** | 0.765 | 0.792 | 0.795 |

**Interpretación de la Generalización**: La mejora en el rendimiento en las Rutas 38 y 49 demuestra que el modelo captura patrones físicos de interacción entre vehículos que son independientes de la geografía específica del corredor. Esto valida la portabilidad del framework algorítmico para arquitecturas de monitoreo regional.

## 6. Benchmarking de Implementación y Latencia
Se midió el costo computacional de la inferencia del modelo (Regresión Logística con ventana n-3) para evaluar su viabilidad en sistemas de tiempo real masivos:

- **Latencia de Inferencia**: ~0.00042 ms por registro.
- **Capacidad de Procesamiento**: > 2.3 millones de eventos/segundo por núcleo de CPU.
- **Recomendación Técnica**: Los resultados empíricos sugieren que una latencia de ingesta de 15s es ideal para el *lead time* operativo, permitiendo al sistema procesar el flujo regional consolidado de una metrópolis con recursos de cómputo mínimos (Edge Computing ready).

## 7. Conclusiones
- Se valida la robustez y generalización del modelo secuencial (n-3) para la predicción de transiciones críticas en múltiples corredores.
- El headway dinámico ($H$) y su derivada temporal consolidan la señal de alerta temprana más fiable para sistemas de Gemelo Digital.
- La eficiencia del modelo permite una implementación escalable para el monitoreo proactivo de flotas urbanas a gran escala.
