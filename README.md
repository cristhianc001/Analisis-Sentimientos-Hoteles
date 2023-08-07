# Analisis de sentimientos y de quejas y reclamos de una cadena hotelera

<p align="center">
<img src="https://img.freepik.com/premium-vector/luxurious-service-satisfied-customer-feedback-positive-review-bed-breakfast_566886-3831.jpg?w=1060" height="350">
</p>

# Tabla de Contenido

- [Autores](#autores)
- [Objetivos y Alcance](#objetivos-y-alcance)
- [KPI's](#kpis)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Tratamiento de Datos](#tratamiento-de-datos)
- [Stack Tecnologico](#stack-tecnologico)
- [Planificacion de esfuerzos](#planificacion-de-esfuerzos)
- [Analisis Exploratorio](#analisis-exploratorio)


## Autores
- [Cristhian Castro](https://www.linkedin.com/in/cristhiancastro/): *Data Scientist*
- [Yaneth Ramirez](): *Data Engineer*
- [Hugo Salazar](): *Data Engineer*
- [Douglas Sanchez](): *Data Analyst*
- [Rodrigo Moreira](): *Data Scientist*

## Objetivos y Alcance

Una cadena de hoteles muy importante de Estados Unidos, que ha solicitado permanecer en el anonimato, solicitó a LATAM DATA CONSULTORES un análisis de reviews y comentarios en diferentes plataformas, con el fin de encontrar oportunidades de mejora para resolver problemas de manera proactiva e impactar en la satisfacción de sus clientes.

Igualmente, realizar un análisis frente a los competidores al comparar y analizar su desempeño y poder tomar medidas para mejorar su ventaja competitiva.

Para el desarrollo de este proyecto se plantea trabajar con análisis de sentimientos y procesamiento de lenguaje natural para encontrar quejas y problemas recurrentes, así como comparar las reviews y calificaciones de los hoteles de la competencia.

## Key Performance Indicators (KPI's)

1.	Porcentaje de revisiones positivas: Medir la proporción de revisiones que son consideradas positivas en comparación con el total de revisiones analizadas. 
2.  Porcentaje de revisiones negativas: Medir la proporción de revisiones que son consideradas negativas en comparación con el total de revisiones analizadas. 
3.  Promedio de calificación: Medir la puntuación promedio dada por lo clientes acerca del servico.

## Estructura del repositorio

- [`notebooks/`](notebooks/): Incluye notebooks de Python para limpieza de datos, EDA y modelos de machine learning.
- [`img/`](img/): Incluye imágenes utilizadas en el readme. como la portada y visualizaciones.


## Tratamiento de Datos
- Extracción de datos de hoteles de Estados Unidos por medio de librerias de Python como json, os, Pandas y AST y usando como fuente archivos iniciales de [Google Maps](https://drive.google.com/drive/folders/1Wf7YkxA0aHI3GpoHc9Nh8_scf5BbD4DA) y [Yelp!](https://drive.google.com/drive/folders/1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF) en su mayoria presentes de forma no estructurada en formato json y fueron transformados a csv.
- Limpieza de dichos datos, lo que incluye eliminación de nulos y duplicados, renombrar columnas, arreglar columnas de fechas y normalización de tablas.
- Análisis exploratorio de los datos para descubrir tendencias, cuotas de mercado, correlaciones, entre otros hallazgos.

## Stack Tecnologico

### Programación
<a href="https://docs.python.org/3/library/ast.html" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> <a href="https://jupyter.org/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/3/38/Jupyter_logo.svg" alt="jupyter" width="40" height="40"/> </a> <a href="https://pandas.pydata.org/docs/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/1119b9f84c0290e0f0b38982099a2bd027a48bf1/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a> <a href="https://numpy.org/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/numpy/numpy-icon.svg" alt="numpy" width="40" height="40"/> </a> <a href="https://huggingface.co/" target="_blank" rel="noreferrer"> <img src="https://huggingface.co/datasets/huggingface/brand-assets/resolve/main/hf-logo.svg" alt="hugging-face" width="40" height="40"/> </a> <a href="https://spacy.io/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/8/88/SpaCy_logo.svg" alt="spacy" width="40" height="40"/> </a> <a href="https://textblob.readthedocs.io/en/dev/" target="_blank" rel="noreferrer"> <img src="https://textblob.readthedocs.io/en/dev/_static/textblob-logo.png" alt="textblob" width="40" height="40"/> </a> 

El lenguaje de programación principal es Python, el cual será usado, en conjunto de cuadernos de jupyter, para tratar los datos, explorarlos y modelarlos. Las librerias a usar para ETL y exploración son Pandas, NumPy, AST, json y os, mientras que en el modelo y procesamiento de lenguaje natural se realizarán iteraciones con [Vader](https://github.com/cjhutto/vaderSentiment), [NLTK](https://www.nltk.org/), spaCy, textblob y modelo pre-entrenado disponibles en Hugging Face.





### Servicio en la nube
<a href="https://cloud.google.com/?hl=es_419" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/google_cloud/google_cloud-icon.svg" alt="google-cloud" width="40" height="40"/> </a>  <a href="https://cloud.google.com/bigquery?hl=es" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/google_bigquery/google_bigquery-icon.svg" alt="bigquery" width="40" height="40"/> </a><a href="https://cloud.google.com/storage?hl=es-419" target="_blank" rel="noreferrer"> <img src="https://symbols.getvecta.com/stencil_4/47_google-cloud-storage.fee263d33a.svg" alt="google-cloud-storage" width="40" height="40"/> </a> <a href="https://cloud.google.com/dataflow?hl=es" target="_blank" rel="noreferrer"> <img src="https://symbols.getvecta.com/stencil_4/57_google-dataflow.aab763346e.svg" alt="scikitlearn" width="40" height="40"/> </a> <a href="https://cloud.google.com/functions" target="_blank" rel="noreferrer"> <img src="https://symbols.getvecta.com/stencil_4/26_google-cloud-functions.3a77982119.svg" alt="scikitlearn" width="40" height="40"/> </a> 

[Google Cloud Platform (GCP)](https://cloud.google.com/free?hl=es) fue elegido como el servicio cloud por encima de Amazon Web Services. En términos económicos, tanto GCP como AWS cobran por uso pero AWS no garantiza un numero de creditos de bienvenida para una prueba de concepto como si lo hace GCP con 300 dolares.  

AWS posee un componente de pago por uso llamado AWS Glue que puede identificar tablas en grandes cantidades y sus schemas  automaticamente, de ser necesario se podrian usar los apartados GCP como Dataflow y Data Catalog para emular esta automatización invirtiendo parte de los 300 dolares que brindan, lo que hace a GCP el servicio mas conveniente para esta demo.

GCP posee una integración con Google Maps que provee de 200 dolares al mes de carga desde su API, lo que constituye un ahorro importante en el mantenimiento del data warehouse y su aprovisionamiento de datos actualizados.

Otra ventaja de GCP sobre AWS es la simpleza, la capacitación del personal a cargo del mantenimiento del pipeline seria mas sencilla a causa de esto. Los servicioes en la nube de Amazon tienen mayor capacidades técnicas y servicios mas maduros como Sagemaker lo que pueder beneficioso para sistemas mas complejos, pero la simpleza de GCP es mas que suficiente para el proyecto teniendo en cuenta el objetivo esta focalizado para los datos de una sola compañia.

### Visualización de datos
<a href="https://matplotlib.org/stable/index.html" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/0/01/Created_with_Matplotlib-logo.svg" alt="matplotlib" width="40" height="40"/> </a>  <a href="https://seaborn.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/gilbarbara/logos/c8749cfc4be0e67a266be0554282d73d967db910/logos/seaborn-icon.svg" alt="seaborn" width="40" height="40"/> </a><a href="https://powerbi.microsoft.com/" target="_blank" rel="noreferrer"> <img src="https://upload.vectorlogo.zone/logos/microsoft_powerbi/images/985205ac-fb3d-4c80-97f4-7bc0fec8c67d.svg" alt="powerbi" width="40" height="40"/> </a> 

El analisis exploratorio de datos se realizó con las librerias de Python Matplotlib y Seaborn, por otra parte para realizar los dashboards se usará Power BI por medio de una conexión con BigQuery, el servicio de data warehouse de GCP.

## Planificacion de esfuerzos

Para visualizar el diagrama en linea, se puede acceder desde este [enlace](https://app.powerbi.com/view?r=eyJrIjoiY2I3MGQ0MTMtYWNlMC00ZGYxLWIwMjMtNGRhMDhiYjkzMzU5IiwidCI6IjYzMmQzMWE5LWIxNWItNDgyNi05ZWQxLTUyYmRmZmI5YjdlNCIsImMiOjl9)
![Diagrama Gantt](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/gantt.png) 

## Analisis Exploratorio

