# Analisis de sentimientos y de quejas y reclamos de una cadena hotelera

<p align="center">
<img src="https://img.freepik.com/premium-vector/luxurious-service-satisfied-customer-feedback-positive-review-bed-breakfast_566886-3831.jpg?w=1060" height="350">
</p>

# Tabla de Contenido

- [Autores](#autores)
- [Introduccion](#introduccion)
- [Objetivos y Alcance](#objetivos-y-alcance)
- [Key Performance Indicators](#key-performance-indicators)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Flujo de Datos](#tratamiento-de-datos)
- [Stack Tecnologico](#stack-tecnologico)
- [Planificacion de esfuerzos](#planificacion-de-esfuerzos)
- [Analisis Exploratorio de Datos](#analisis-exploratorio-de-datos)

## Autores

***Data Analyst***:
[Douglas Sanchez](https://www.linkedin.com/in/douglassanchezcasanova/)

Como Data Analyst, desempeñó un papel crucial en la interpretación y análisis profundo de los datos recopilados. Utilizó su habilidad para detectar patrones y tendencias en las reseñas y comentarios de los clientes, proporcionando información valiosa para la toma de decisiones informadas.

***Data Engineers***: [Yaneth Ramirez]( https://www.linkedin.com/in/yanethramirez/), [Hugo Salazar](https://www.linkedin.com/in/hasalazars/) 

El equipo de Data Engineers, liderado por Yaneth Ramírez y Hugo Salazar, asumió la responsabilidad de la recopilación, limpieza y preparación de los datos. Gracias a su experiencia en ingeniería de datos, aseguraron que los datos estuvieran listos para su análisis, permitiendo un flujo eficiente desde la extracción hasta la interpretación.

***Data Scientists***: 
[Cristhian Castro](https://www.linkedin.com/in/cristhiancastro/), [Rodrigo Moreira](https://www.linkedin.com/in/rcmoreg)

Los Data Scientists del equipo, se centraron en la aplicación de algoritmos de procesamiento de lenguaje natural y análisis de sentimientos en las reseñas. Su experiencia permitió la identificación de quejas recurrentes y la extracción de información valiosa para el cliente.

## Introduccion

Una cadena de hoteles muy importante de Estados Unidos solicitó a LATAM DATA CONSULTORES un análisis de reviews y comentarios en diferentes plataformas, con el fin de encontrar oportunidades de mejora para resolver problemas de manera proactiva e impactar en la satisfacción de sus clientes.


## Objetivos y Alcance

El **objetivo** principal es identificar y abordar las áreas problemáticas que afectan la satisfacción del cliente. El análisis de sentimientos puede ayudar a detectar las principales preocupaciones o feedback negativo de los huéspedes.

A través de técnicas de análisis de sentimientos y procesamiento de lenguaje natural, buscamos obtener información valiosa para mejorar la satisfacción del cliente, identificar oportunidades de mejora y resolver problemas de manera proactiva.

**Objetivos Específicos**:
Evaluar la satisfacción general de los huéspedes: Utilizando el análisis de sentimientos, buscamos medir la proporción de revisiones que son consideradas positivas en comparación con el total de revisiones analizadas. Esto nos permitirá comprender la satisfacción general de los clientes con los servicios ofrecidos por la cadena hotelera.
 
-	Identificar problemas y quejas recurrentes: A través del procesamiento de lenguaje natural, buscaremos identificar temas y quejas recurrentes en las revisiones. Estas quejas pueden incluir problemas con el servicio al cliente, problemas en las habitaciones, demoras en el check-in, entre otros. Al identificar estos problemas, podremos tomar medidas para resolverlos y mejorar la experiencia del cliente.

-	Realizar un análisis comparativo con la competencia: Además de analizar las revisiones de la cadena hotelera, también compararemos su desempeño con los hoteles de la competencia. Esto nos permitirá determinar en qué áreas la cadena se destaca y en qué aspectos puede mejorar para mantener o mejorar su ventaja competitiva.


**Alcance del Proyecto**: Para llevar a cabo este proyecto, trabajaremos con datos de revisiones y comentarios recopilados de plataformas como Yelp, Google Maps, Outscraper, Hotels.com. Estos datos serán sometidos a técnicas de análisis de sentimientos y procesamiento de lenguaje natural.

El análisis se centrará en el período 2018 - 2023 y se limitará a las revisiones relacionadas con los servicios ofrecidos por la cadena hotelera, incluyendo reviews sobre las habitaciones, el servicio al cliente, el restaurante, entre otros. Las revisiones se clasificarán como positivas, negativas o neutrales en función del análisis de sentimientos realizado.


## Key Performance Indicators

1.	Porcentaje de revisiones positivas: Medir la proporción de revisiones que son consideradas positivas en comparación con el total de revisiones analizadas. 
2.  Porcentaje de revisiones negativas: Medir la proporción de revisiones que son consideradas negativas en comparación con el total de revisiones analizadas. 
3.  Puntuación promedio de sentimiento: Calcular la puntuación promedio de sentimiento de las revisiones, asignando valores a las palabras clave o frases que reflejen emociones positivas o negativas. Esto permite medir el nivel general de satisfacción y detectar tendencias.
4. Número de menciones positivas/negativas por categoría de servicio: Analizar las revisiones en función de las diferentes categorías de servicios del hotel (habitaciones, restaurante, atención al cliente, etc.) y contar el número de menciones positivas y negativas en cada categoría. Esto ayuda a identificar las áreas específicas que están generando más comentarios emocionales.
5.	Comparación de sentimiento con la competencia: Comparar el sentimiento de las revisiones de tu hotel con el de los competidores directos. Esto proporciona una visión comparativa y ayuda a identificar áreas en las que se puede aprender de la competencia o destacar por encima de ella.

## Estructura del repositorio

- [`notebooks/`](notebooks/): Incluye notebooks de Python para limpieza de datos, EDA y modelos de machine learning.
- [`img/`](img/): Incluye imágenes utilizadas en el readme. como la portada y visualizaciones.


## Flujo de Datos
- Extracción de datos de hoteles de Estados Unidos por medio de librerias de Python como json, os, Pandas y AST y usando como fuente archivos iniciales de [Google Maps](https://drive.google.com/drive/folders/1Wf7YkxA0aHI3GpoHc9Nh8_scf5BbD4DA) y [Yelp!](https://drive.google.com/drive/folders/1TI-SsMnZsNP6t930olEEWbBQdo_yuIZF) en su mayoria presentes de forma no estructurada en formato json y fueron transformados a csv.
- Limpieza de dichos datos, lo que incluye eliminación de nulos y duplicados, renombrar columnas, arreglar columnas de fechas y normalización de tablas.
- Análisis exploratorio de los datos para descubrir tendencias, cuotas de mercado, correlaciones, entre otros hallazgos.
- Ingesta de datos en el servicio de almacenamiento de Google Cloud.
- Construcción de Data Warehouse automatizado con el batch de datos provenientes de Google Cloud Storage, incluyendo carga incremental con data stream proveniento de las API de Google Maps y Yelp!
- Consulta a las bases de datos del data warehouse para construir modelo de aprendizaje y dashboards.

## Stack Tecnologico

### Gestión y Comunicación
<a href="https://discord.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/discordapp/discordapp-tile.svg" alt="discord" width="40" height="40"/> </a> <a href="https://meet.google.com/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/9/9b/Google_Meet_icon_%282020%29.svg" alt="google-meet" width="40" height="40"/> </a> <a href="https://git-scm.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="40" height="40"/> </a> <a href="https://github.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/github/github-icon.svg" alt="github" width="40" height="40"/> </a> 
<a href="https://trello.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/trello/trello-tile.svg" alt="trello" width="40" height="40"/> </a> 

La coordinación del flujo de trabajo se realizó por medio reuniones diarias a traves de Google Meet y Discord, Trello se usó para listar las actividades del proyecto, Git como controlador de versiones y Github como repositorio remoto.

### Programación
<a href="https://docs.python.org/3/library/ast.html" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> <a href="https://jupyter.org/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/3/38/Jupyter_logo.svg" alt="jupyter" width="40" height="40"/> </a> <a href="https://pandas.pydata.org/docs/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/1119b9f84c0290e0f0b38982099a2bd027a48bf1/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a> <a href="https://numpy.org/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/numpy/numpy-icon.svg" alt="numpy" width="40" height="40"/> </a> <a href="https://huggingface.co/" target="_blank" rel="noreferrer"> <img src="https://huggingface.co/datasets/huggingface/brand-assets/resolve/main/hf-logo.svg" alt="hugging-face" width="40" height="40"/> </a> <a href="https://spacy.io/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/8/88/SpaCy_logo.svg" alt="spacy" width="40" height="40"/> </a> <a href="https://textblob.readthedocs.io/en/dev/" target="_blank" rel="noreferrer"> <img src="https://textblob.readthedocs.io/en/dev/_static/textblob-logo.png" alt="textblob" width="40" height="40"/> </a> </a><a href="https://powerbi.microsoft.com/" target="_blank" rel="noreferrer"> <img src="https://upload.vectorlogo.zone/logos/microsoft_powerbi/images/985205ac-fb3d-4c80-97f4-7bc0fec8c67d.svg" alt="powerbi" width="40" height="40"/> </a> <a href="https://developer.mozilla.org/es/docs/Web/HTML" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/w3_html5/w3_html5-icon.svg" alt="html" width="40" height="40"/> </a> 

El lenguaje de programación principal es Python, el cual será usado, en conjunto de cuadernos de jupyter, para tratar los datos, explorarlos y modelarlos. Las librerias a usar para ETL y exploración son Pandas, NumPy, AST, json y os, mientras que en el modelo y procesamiento de lenguaje natural se realizarán iteraciones con [Vader](https://github.com/cjhutto/vaderSentiment), [NLTK](https://www.nltk.org/), spaCy, textblob y modelo pre-entrenado disponibles en Hugging Face. También se usara DAX y HTML para la visualización de datos del producto final.



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
Adoptamos la metodología ágil Scrum para la ejecución del proyecto. El equipo se estructuró en ciclos de trabajo denominados "sprints" con una duración de dos semanas. Se llevaron a cabo reuniones diarias de seguimiento y al final de cada sprint, se efectuó una sesión de revisión. El cronograma completo está reflejado en el siguiente diagrama de Gantt.

Para visualizar el diagrama en linea, se puede acceder desde este [enlace](https://app.powerbi.com/view?r=eyJrIjoiYTcxMTk5NDYtM2M0OS00YTI0LTkxNWYtZWQzZTljMjUxNTg4IiwidCI6IjYzMmQzMWE5LWIxNWItNDgyNi05ZWQxLTUyYmRmZmI5YjdlNCIsImMiOjl9).
![Diagrama Gantt](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/gantt.png) 

## Analisis Exploratorio de Datos

A continuación se muestran algunos insights hallados en los EDA, para consultar la información completa consultar los archivos de analisis exploratorio de datos en  [`notebooks/`](notebooks/).

La mayoria de las hoteles de la data extraida de Google Maps se encuentran en los estados de California, Florida, Nueva York y Texas, lo cual concuerda con la realidad pues son los estados mas turisticos de Estados Unidos.

![Hoteles por Estado](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/barplot-hoteles-estados.png)
*Hoteles por Estado en dataset de Google Maps*

A pesar de que no hayan muchos hoteles registrados en Oklahoma, este estado esta sobre-representado con muchas reviews en pocos hoteles como se muestra en las siguientes figuras.

![Reviews por Estado](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/barplot-reviews-estados.png)
*Reviews por Estado en dataset de Google Maps*

![Reviews por Estado](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/ratio-reviews-hoteles.png)
*Ratio reviews/hoteles por Estado en dataset de Google Maps*

En términos generales hay correlación entre número de reviews con el número de hoteles, excepto por  outliers como Oklahoma con 45 hoteles y 3761 reviews y Colorado con 57 hoteles y 2919 reviews

![Correlacion por Reviews](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/correlacion-hoteles-reviewspng.png)
*Correlación entre numero de reviews y numero de hoteles en dataset de Google Maps*

El analisis por tiempo indica una reduccion de review en el 2019 y un desplome posterior en el 2020 a causa de la pandemia.

![Reviews por año](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/lineas-reviews-a%C3%B1o.png)
*Correlación entre numero de reviews y numero de hoteles en dataset de Google Maps*

El número de reviews mes por mes nos indica que los meses de mayor ocupación en Hoteles en Estados Unidos se da en periodos vacacionales. Encontramos un pequeño pico en Marzo que concide con Spring Break y en vacaciones de Verano entre Julio y Agosto. Por otra parte, el pico más bajo se encuentra en el mes de Noviembre, coincidiendo con Thanks Giving, fecha en la cual todas las personas pasan tiempo con familiares y amigos en sus respectivos hogares.

![Reviews por mes](https://raw.githubusercontent.com/cristhianc001/Analisis-Sentimientos-Hoteles/main/img/lineas-meses-reviews.png)
*Correlación entre numero de reviews y numero de hoteles en dataset de Google Maps*