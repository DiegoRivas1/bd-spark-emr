# Análisis Distribuido con PySpark en Amazon EMR

**Curso:** BigData 2026
**Alumno:** Rivas Huanca, Diego Raúl

> Configuración SSH y Wave: ver [dev-environment-setup](https://github.com/DiegoRivas1/dev-environment-setup.git)
> Laboratorio anterior (Hive): ver [bd-hive-emr](https://github.com/DiegoRivas1/bd-hive-emr.git)

---

## Arquitectura del clúster

- Amazon EMR 7.13.0, Spark 3.5.6, Hadoop 3.4.2, Hive 3.1.3
- 1 nodo master m5.xlarge (4 vCPU, 16 GB RAM)
- 3 nodos worker Core m5.xlarge (4 vCPU, 16 GB RAM c/u)
- Almacenamiento: Amazon S3

---

## Datasets utilizados

| Dataset | Tamaño | Registros | Descripción |
|---------|--------|-----------|-------------|
| Project Gutenberg | ~1.5 MB | 3 archivos | Pride and Prejudice, Alice in Wonderland, Sherlock Holmes |
| Simple Wikipedia | ~1.7 GB | 4 archivos combinados | 293,808 artículos extraídos del dump oficial |
| NYC Yellow Taxi | ~1.7 GB | 104,770,192 viajes | 28 meses 2024-2026, particionado year/month |
| NYC FHVHV (Uber/Lyft) | ~13 GB | 566,930,502 viajes | 28 meses 2024-2026, particionado year/month |

### Preparación del dataset Wikipedia

```bash
# Descargar dump Simple Wikipedia
cd /mnt
wget -q https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-articles.xml.bz2
bzip2 -d simplewiki-latest-pages-articles.xml.bz2
# Resultado: 1.6 GB XML, 30 millones de lineas

# Convertir XML a archivos txt (293,808 articulos)
cd ~
aws s3 cp s3://lab03-indice-invertido/scripts/parse_wiki.py .
sed -i 's/max_docs = 5000/max_docs = 999999/' parse_wiki.py
python3 parse_wiki.py

# Combinar en 1 archivo de 428 MB
mkdir -p /mnt/wiki_combined_large
find /mnt/wiki_docs -name "*.txt" -print0 | \
  xargs -0 cat > /mnt/wiki_combined_large/wiki_part_1.txt

# Duplicar para llegar a ~1.7 GB
cp /mnt/wiki_combined_large/wiki_part_1.txt /mnt/wiki_combined_large/wiki_part_2.txt
cp /mnt/wiki_combined_large/wiki_part_1.txt /mnt/wiki_combined_large/wiki_part_3.txt
cp /mnt/wiki_combined_large/wiki_part_1.txt /mnt/wiki_combined_large/wiki_part_4.txt

# Subir a S3
aws s3 cp /mnt/wiki_combined_large/ \
  s3://lab03-indice-invertido/wiki_combined_large/ --recursive
```

> Nota: se usan 4 archivos grandes en vez de 293,808 archivos individuales para evitar
> el overhead de listar millones de objetos en S3.

---

## Estructura S3

```
s3://lab03-indice-invertido/
├── input/                      3 libros Gutenberg
├── wiki_combined_large/        4 archivos Wikipedia ~1.7 GB
├── taxi_partitioned/           Yellow Taxi 28 meses (year=/month=)
├── fhvhv_partitioned/          FHVHV 28 meses (year=/month=)
└── spark_scripts/              Scripts PySpark
```

---

## Cómo ejecutar los scripts

```bash
# Recuperar scripts de S3
aws s3 cp s3://lab03-indice-invertido/spark_scripts/ ~/ --recursive

# Ejecutar
spark-submit wordcount.py
spark-submit --driver-memory 4g --executor-memory 6g wiki_combined_large.py
spark-submit taxi_analysis.py
spark-submit fhvhv_analysis.py
```

Ver [dev-environment-setup](https://github.com/DiegoRivas1/dev-environment-setup.git) para configurar SSH y Wave.

---

## Ejercicio 1 WordCount

### Gutenberg (~1.5 MB)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, length

spark = SparkSession.builder.appName("WordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.text("s3://lab03-indice-invertido/input/")

words = df.select(
    explode(split(lower(col("value")), "[^a-z]+")).alias("word")
).filter((col("word") != "") & (length(col("word")) > 2))

result = words.groupBy("word").count().orderBy("count", ascending=False)
result.show(20)
```

**Tiempo: 14 seg**

### Wikipedia (~1.7 GB) comparativa 3 tecnologías

**Spark:**
```bash
time spark-submit --driver-memory 4g --executor-memory 6g wiki_combined_large.py
```

**Hive:**
```sql
CREATE EXTERNAL TABLE wiki_text (line STRING)
STORED AS TEXTFILE
LOCATION 's3://lab03-indice-invertido/wiki_combined_large/';

SELECT word, COUNT(*) AS freq
FROM (
    SELECT explode(split(lower(line), '[^a-z]+')) AS word
    FROM wiki_text
) t
WHERE length(word) > 2
GROUP BY word
ORDER BY freq DESC
LIMIT 20;
```

**MapReduce:**
```bash
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapper.py,reducer.py \
  -mapper mapper.py \
  -reducer reducer.py \
  -input s3://lab03-indice-invertido/wiki_combined_large/ \
  -output /user/hadoop/wc_output
```

**Resultados WordCount Wikipedia:**
```
+----------+--------+
|word      |count   |
+----------+--------+
|the       |11504088|
|and       | 4450484|
|category  | 3565704|
|was       | 2087480|
|for       | 1622936|
+----------+--------+
```

| Tecnología | Tiempo |
|-----------|--------|
| Spark | 56.11 seg |
| Hive | 130.23 seg |
| MapReduce | 664.54 seg (11 min) |

---

## Ejercicio 2 — Índice Invertido

### Gutenberg (~1.5 MB)

```python
from pyspark.sql.functions import explode, split, lower, col, length
from pyspark.sql.functions import input_file_name, regexp_extract, collect_set

df = spark.read.text("s3://lab03-indice-invertido/input/") \
    .withColumn("filename", regexp_extract(input_file_name(), r"([^/]+)$", 1))

words = df.select(
    explode(split(lower(col("value")), "[^a-z]+")).alias("word"),
    col("filename")
).filter((col("word") != "") & (length(col("word")) > 2))

result = words.groupBy("word") \
    .agg(collect_set("filename").alias("documentos")) \
    .orderBy("word")

result.show(20, truncate=False)
```

**Tiempo: 12 seg**

### Wikipedia (~1.7 GB) — comparativa 3 tecnologías

**Spark:**
```python
df = spark.read.text("s3://lab03-indice-invertido/wiki_combined_large/") \
    .withColumn("doc", input_file_name())

index_df = df.select(
    explode(split(lower(col("value")), "[^a-z]+")).alias("word"),
    col("doc")
).filter(col("word") != "") \
 .groupBy("word") \
 .agg(collect_set("doc").alias("documents"))

index_df.show(20, truncate=False)
```

**Hive:**
```sql
SELECT word, collect_set(INPUT__FILE__NAME) AS docs
FROM wiki_text
LATERAL VIEW explode(split(lower(line), '[^a-z]+')) e AS word
WHERE length(word) > 2
GROUP BY word
LIMIT 20;
```

**MapReduce:**
```bash
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -files mapper_inverted.py,reducer_inverted.py \
  -mapper mapper_inverted.py \
  -reducer reducer_inverted.py \
  -input s3://lab03-indice-invertido/wiki_combined_large/ \
  -output /user/hadoop/inverted_output

hdfs dfs -cat /user/hadoop/inverted_output/part-* | head -20
```

| Tecnología | Tiempo |
|-----------|--------|
| Hive | 206.56 seg |
| Spark | 219.34 seg |
| MapReduce | 712.58 seg (11 min 52 seg) |

> Hive superó ligeramente a Spark en índice invertido porque Tez ejecutó
> el collect_set de forma muy eficiente con solo 4 archivos grandes.

---

## Ejercicio 3 — Análisis Comparativo Final

### WordCount e Índice Invertido

| Operación | Dataset | Spark | Hive | MapReduce |
|-----------|---------|-------|------|-----------|
| WordCount | 1.5 MB Gutenberg | 14 seg | 13 seg | ~18 seg |
| WordCount | 1.7 GB Wikipedia | 56 seg | 130 seg | 665 seg |
| Índice invertido | 1.5 MB Gutenberg | 12 seg | 1.8 seg | ~18 seg |
| Índice invertido | 1.7 GB Wikipedia | 219 seg | 206 seg | 713 seg |

### NYC Taxi

| Operación | Dataset | Spark | Hive | MapReduce |
|-----------|---------|-------|------|-----------|
| 5 consultas analíticas | 1.7 GB / 104M viajes | ~34 seg total | ~100 seg c/u | no aplica* |
| 4 consultas analíticas | 13 GB / 566M viajes | ~2 min total | ~150 seg c/u | no aplica* |
| SIN partición COUNT | 1.7 GB Yellow Taxi | 1.15 seg | 15.57 seg | no aplica* |
| CON partición COUNT | 250 MB (2026) | 0.65 seg | 5.28 seg | no aplica* |
| SIN partición COUNT | 13 GB FHVHV | 0.51 seg | 114 seg | no aplica* |
| CON partición COUNT | 1.9 GB (2026) | 0.34 seg | 30 seg | no aplica* |

*MapReduce no aplica para análisis SQL — requeriría un mapper/reducer custom por cada consulta.

### Conclusiones

**Spark es el más rápido en datos grandes:** 11.8x más rápido que MapReduce en WordCount.
Usa procesamiento en memoria con Catalyst Optimizer y Tungsten Engine.

**Hive es competitivo con datos medianos:** superó a Spark en índice invertido con 4 archivos
grandes gracias a Tez. Es la mejor opción cuando el equipo conoce SQL y los datos son estructurados.

**MapReduce es el más lento:** escribe resultados intermedios a disco entre Map y Reduce.
Sigue siendo útil para lógica muy personalizada que SQL no puede expresar.

**El particionamiento beneficia más a Hive que a Spark:** Spark ya es tan rápido leyendo
parquet que el partition pruning reduce el tiempo de 1.15 seg a 0.65 seg (1.8x).
En Hive la mejora es de 15.57 seg a 5.28 seg (3x).

---

## Ejercicio 4 — NYC Taxi con Spark

### Yellow Taxi particionado (28 meses, 104M viajes)

```python
df = spark.read.parquet("s3://lab03-indice-invertido/taxi_partitioned/")

# Total viajes por año
df.groupBy("year").count().orderBy("year").show()

# Horas pico 2025
df.filter(col("year") == 2025) \
  .withColumn("hora", hour(col("tpep_pickup_datetime"))) \
  .groupBy("hora").count().orderBy(desc("count")).show(5)
```

**Resultados Yellow Taxi:**
```
+----+--------+          +----+-------+
|year|   count|          |hora|  count|
+----+--------+          +----+-------+
|2024|41169720|          |  18|3473210|
|2025|48722602|          |  17|3291155|
|2026|14877870|          |  19|3071607|
+----+--------+          +----+-------+
```

### FHVHV Uber/Lyft particionado (28 meses, 566M viajes)

```python
df = spark.read.parquet("s3://lab03-indice-invertido/fhvhv_partitioned/")

# Total viajes por año
df.groupBy("year").count().orderBy("year").show()
```

**Resultados FHVHV:**
```
+----+---------+
|year|    count|
+----+---------+
|2024|239470448|
|2025|243589684|
|2026| 83870370|
TOTAL: 566,930,502
+----+---------+
```

---

## Capturas

### Cluster EMR activo
![cluster](capturas/01_cluster_emr.png)

### WordCount Gutenberg — Spark
![wc_gutenberg](capturas/02_wordcount_gutenberg.png)

### WordCount Wikipedia — comparativa
![wc_wiki](capturas/03_wordcount_wikipedia_comparativa.png)

### Índice invertido Gutenberg — Spark
![ii_gutenberg](capturas/04_indice_invertido_gutenberg.png)

### Índice invertido Wikipedia — comparativa
![ii_wiki](capturas/05_indice_invertido_wikipedia_comparativa.png)

### Yellow Taxi — consultas analíticas
![yellow](capturas/06_yellow_taxi_consultas.png)

### Yellow Taxi — particionamiento
![yellow_part](capturas/07_yellow_taxi_particion.png)

### FHVHV — consultas analíticas
![fhvhv](capturas/08_fhvhv_consultas.png)

### FHVHV — particionamiento
![fhvhv_part](capturas/09_fhvhv_particion.png)
