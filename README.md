# Airflow + BigQuery + dbt — Retail Data Engineering Pipeline

Pipeline de Ingeniería de Datos construido con **Apache Airflow (Astro)**, **BigQuery**, **dbt Core**, **GitHub Actions**, **pytest** y **Ruff**.

El proyecto utiliza el dataset **Online Retail** y demuestra un flujo completo desde la ingesta de un archivo Excel hasta un modelo dimensional en BigQuery, incluyendo pruebas de calidad y CI/CD.

---

## Arquitectura

```text
                    GitHub
                       │
                GitHub Actions
                 ┌─────┴─────┐
                 │           │
               Ruff       pytest
                 │           │
                 └─────┬─────┘
                       ▼
                    Airflow
                  (Astro/Docker)
                       │
              ┌────────┴────────┐
              ▼                 ▼
          extract              load
              │                 │
              ▼                 │
    Online Retail.xlsx          │
              │                 │
       Pandas + PyArrow         │
              │                 │
              ▼                 │
   Online_Retail.parquet        │
              └────────┬────────┘
                       ▼
                    BigQuery
                       │
               raw_online_retail
                       │
                       ▼
                      dbt
                       │
                 stg_online_retail
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
    dim_customer   dim_product   dim_date
          └────────────┬────────────┘
                       ▼
                   fact_sales
                       │
                   dbt tests
                       │
                    SUCCESS
```

---

## Objetivos del proyecto

- Practicar **orquestación con Airflow**.
- Ingestar datos reales hacia **BigQuery**.
- Separar la capa **RAW** de las transformaciones.
- Utilizar **dbt** para staging, modelado dimensional y pruebas.
- Implementar **CI/CD** con GitHub Actions.
- Aplicar **linting** con Ruff.
- Aplicar **testing** con pytest y dbt tests.
- Trabajar autenticación mediante una **Google Cloud Service Account** sin exponer credenciales en Git.

---

## Dataset

Se utiliza el dataset **Online Retail**.

Archivo fuente:

```text
Online Retail.xlsx
```

El archivo se conserva como dato RAW en:

```text
raw/data/Online Retail.xlsx
```

Características observadas durante el EDA:

- 541,909 registros.
- 8 columnas.
- 5,268 registros duplicados.
- 135,080 `CustomerID` nulos.
- 9,288 facturas cuyo `InvoiceNo` comienza con `C`.
- 10,624 registros con `Quantity < 0`.
- 2,517 registros con `UnitPrice <= 0`.
- 4,372 clientes identificados.
- 2,517 códigos de producto distintos.
- Periodo aproximado: diciembre de 2010 a diciembre de 2011.

Durante el EDA también se detectaron columnas con tipos mixtos, especialmente `InvoiceNo`, `StockCode` y `Description`. Esto llevó a realizar normalizaciones mínimas durante la ingesta para poder escribir el Parquet de forma consistente.

---

## Stack tecnológico

| Tecnología | Uso |
|---|---|
| **Python** | Lógica de ingesta |
| **Pandas** | Lectura y preparación del Excel |
| **PyArrow / Parquet** | Formato intermedio columnar |
| **Apache Airflow / Astro** | Orquestación |
| **Docker** | Runtime local de Airflow |
| **Google BigQuery** | Data Warehouse |
| **dbt Core** | Transformación y modelado |
| **dbt-bigquery** | Adaptador de dbt para BigQuery |
| **pytest** | Tests de Python/Airflow |
| **Ruff** | Linting y calidad de código |
| **GitHub Actions** | CI/CD |

---

## Flujo de Airflow

El DAG principal es:

```text
sales_pipeline
```

El flujo final es:

```text
extract >> load >> dbt_run >> dbt_test
```

### `extract`

Responsabilidades:

1. Localizar el archivo `Online Retail.xlsx`.
2. Leerlo con Pandas.
3. Normalizar tipos necesarios para evitar errores de PyArrow.
4. Generar el archivo Parquet intermedio.

Normalizaciones aplicadas durante la ingesta:

```python
df["InvoiceNo"] = df["InvoiceNo"].astype(str)
df["StockCode"] = df["StockCode"].astype(str)
df["Description"] = df["Description"].astype("string")
```

El resultado se guarda en:

```text
processed/Online_Retail.parquet
```

### `load`

Lee el Parquet y lo carga a BigQuery mediante `BigQueryHook` y el cliente de BigQuery.

Tabla RAW:

```text
mi-dw-123456.online_retail.raw_online_retail
```

La carga utiliza:

```text
WRITE_TRUNCATE
```

para reemplazar el contenido de la tabla en cada ejecución.

### `dbt_run`

Airflow ejecuta dbt dentro del contenedor de Astro mediante `BashOperator`:

```bash
cd /usr/local/airflow/dbt_project && dbt run
```

### `dbt_test`

Después del `dbt run`, Airflow ejecuta:

```bash
cd /usr/local/airflow/dbt_project && dbt test
```

Esto permite que el DAG falle si los modelos transformados no cumplen las reglas de calidad configuradas.

---

## BigQuery

Se creó el dataset:

```text
online_retail
```

La tabla RAW es:

```text
raw_online_retail
```

Esquema observado:

| Campo | Tipo BigQuery |
|---|---|
| `InvoiceNo` | STRING |
| `StockCode` | STRING |
| `Description` | STRING |
| `Quantity` | INTEGER |
| `InvoiceDate` | DATETIME |
| `UnitPrice` | FLOAT |
| `CustomerID` | FLOAT |
| `Country` | STRING |

---

## Autenticación de Google Cloud

Se utilizó una **Service Account** dedicada:

```text
airflow-bigquery
```

Con permisos para ejecutar trabajos de BigQuery y trabajar con los datos del proyecto/dataset utilizado.

La conexión de Airflow se configuró como:

```text
google_cloud_default
```

Las credenciales reales **no deben subirse al repositorio**.

El archivo local utilizado durante desarrollo es:

```text
include/gcp/credentials.json
```

y debe estar incluido en `.gitignore`.

> Nunca publicar una clave real de Google Cloud en GitHub.

---

## dbt

Se utilizó:

```text
dbt Core 1.12.2
dbt-bigquery 1.12.0
```

La conexión de dbt utiliza `service-account` y el dataset:

```text
online_retail
```

con ubicación:

```text
US
```

### `source()`

`raw_online_retail` fue creada por Airflow, por lo que se declaró como **source** de dbt.

Ejemplo conceptual:

```jinja
{{ source('raw', 'raw_online_retail') }}
```

### `ref()`

Los modelos creados por dbt se referencian utilizando `ref()`.

Ejemplo:

```jinja
{{ ref('stg_online_retail') }}
```

Esto permite que dbt conozca automáticamente las dependencias entre modelos.

---

## Modelos dbt

### Staging

#### `stg_online_retail`

Modelo de staging que:

- estandariza nombres a `snake_case`;
- convierte `CustomerID` a `INT64`;
- conserva los datos útiles para el modelo analítico;
- filtra registros según las reglas iniciales de ventas.

Regla de ventas utilizada en staging:

```sql
Quantity > 0
AND UnitPrice > 0
AND NOT STARTS_WITH(CAST(InvoiceNo AS STRING), 'C')
```

El modelo resultó en aproximadamente **530,104 filas**.

### Dimensiones

#### `dim_customer`

Grano:

```text
una fila por customer_id
```

Se excluyen clientes sin identificador:

```sql
WHERE customer_id IS NOT NULL
```

Cuando un cliente aparece asociado a varios países, se selecciona el país con mayor frecuencia de transacciones mediante una ventana con `ROW_NUMBER()`.

#### `dim_product`

Grano:

```text
stock_code + description
```

Durante los tests se descubrió que `stock_code` por sí solo **no es único**. Por eso se decidió utilizar la combinación `stock_code + description` como grano lógico del producto.

#### `dim_date`

Se obtiene a partir de `invoice_date` y contiene:

```text
date
year
month
day
```

### Fact

#### `fact_sales`

Grano:

```text
una fila por línea de factura/venta
```

Columnas principales:

```text
invoice_no
customer_id
stock_code
date
quantity
unit_price
revenue
```

`revenue` se calcula como:

```sql
quantity * unit_price
```

La tabla se relaciona con:

```text
fact_sales.customer_id  → dim_customer.customer_id
fact_sales.stock_code   → dim_product.stock_code
generated date          → dim_date.date
```

---

## Data Quality con dbt

Se configuraron tests sobre los modelos.

### `dim_customer`

```text
customer_id → not_null
customer_id → unique
```

Los tests inicialmente fallaron, lo que permitió detectar:

- clientes con `customer_id` nulo;
- clientes duplicados por país.

Se corrigió el modelo utilizando una regla de frecuencia por país y posteriormente los tests pasaron.

### `dim_product`

```text
stock_code → not_null
```

La prueba de unicidad sobre `stock_code` falló con 220 duplicados. El EDA mostró que existían varias descripciones para un mismo `stock_code`, por lo que se redefinió el grano como:

```text
stock_code + description
```

Se creó además un **test singular** para comprobar la unicidad de esa combinación.

### `dim_date`

```text
date → not_null
date → unique
```

### `fact_sales`

```text
invoice_no → not_null
quantity   → not_null
revenue    → not_null
```

Todos los tests implementados terminaron pasando.

---

## CI/CD

GitHub Actions ejecuta validaciones automáticas del proyecto.

Flujo principal:

```text
push / pull request
        ↓
GitHub Actions
        ├── instalar Python
        ├── instalar dependencias
        ├── Ruff
        └── pytest
```

Ejemplo conceptual:

```yaml
- name: Run Ruff
  run: ruff check dags tests

- name: Run tests
  run: pytest
```

Esto permite detectar problemas de estilo y regresiones antes de integrar cambios.

---

## Estructura del proyecto

Estructura conceptual utilizada durante el desarrollo:

```text
airflow-cicd/
├── dags/
│   └── sales_pipeline.py
│
├── raw/
│   └── data/
│       └── Online Retail.xlsx
│
├── processed/
│   └── Online_Retail.parquet
│
├── include/
│   └── gcp/
│       └── credentials.json        # NO subir a Git
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                 # entorno local/contendedor; revisar secretos
│   ├── models/
│   │   └── staging/
│   │       ├── sources.yml
│   │       ├── stg_online_retail.sql
│   │       ├── dim_customer.sql
│   │       ├── dim_product.sql
│   │       ├── dim_date.sql
│   │       ├── fact_sales.sql
│   │       ├── schema.yml
│   │       └── tests/
│   │           └── dim_product_unique.sql
│   └── target/
│
├── tests/
│   └── dags/
│       └── test_dag.py
│
├── .github/
│   └── workflows/
│       └── ...
│
├── Dockerfile
├── packages.txt
├── requirements.txt
├── pyproject.toml
├── .gitignore
└── README.md
```

> La organización puede compactarse posteriormente moviendo las dimensiones/fact a una carpeta `marts/`. La implementación funcional del proyecto ya está completada.

---

## Comandos principales

### Airflow / Astro

Iniciar el entorno:

```bash
astro dev start
```

Ver contenedores:

```bash
astro dev ps
```

Entrar al contenedor:

```bash
astro dev bash
```

### Ruff

```bash
ruff check dags tests
```

### pytest

```bash
pytest
```

### dbt

Desde `dbt_project/`:

```bash
dbt debug
dbt run
dbt test
```

Ejecutar un modelo específico:

```bash
dbt run --select stg_online_retail
dbt run --select dim_customer
dbt run --select dim_product
dbt run --select dim_date
dbt run --select fact_sales
```

Generar documentación localmente requiere utilizar el `profiles.yml` correspondiente al entorno desde el que se ejecuta dbt.

---

## Decisiones de diseño relevantes

### ¿Por qué Parquet?

El Excel se utiliza como fuente original. Se genera un Parquet intermedio porque es un formato columnar adecuado para pipelines analíticos y evita mantener el Excel como formato de intercambio entre etapas.

### ¿Por qué `source()`?

`raw_online_retail` es una tabla creada fuera de dbt por Airflow.

### ¿Por qué `ref()`?

Los modelos como `stg_online_retail`, `dim_customer`, `dim_product`, `dim_date` y `fact_sales` son creados por dbt. `ref()` permite declarar sus dependencias.

### ¿Por qué las dimensiones son tablas físicas?

Se utilizó:

```jinja
{{ config(materialized='table') }}
```

para poder inspeccionar los modelos físicamente en BigQuery durante el aprendizaje.

### ¿Por qué `invoice_no` no es unique en `fact_sales`?

Una factura puede contener varias líneas de productos. Por eso el grano de la tabla de hechos es una línea de venta, no una factura completa.

---

## Resultado final

El proyecto demuestra una arquitectura de Data Engineering con:

```text
Ingesta
   ↓
Orquestación
   ↓
BigQuery RAW
   ↓
Transformación con dbt
   ↓
Modelo dimensional
   ↓
Tests de calidad
   ↓
CI/CD
```

Tecnologías principales:

**Airflow + Astro + BigQuery + dbt + Python + Pandas + Parquet + pytest + Ruff + GitHub Actions**.

