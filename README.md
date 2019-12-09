# Usos
El script tweets_sentiments.py hace uso del paradigma de computación distribuida MapReduce para realizar un análisis de sentimientos 
utilizando para ello tweets recabados a través de la API de Twitter. En concreto, se utiliza MRJob, que permite simular el 
comportamiento de un sistema distribuido en local, así como ejecutar en entornos distribuidos reales (como puede ser Cloudera)
y con el servicio EMR de AWS.
Tiene 3 modos de funcionamiento, que se especifican con el parámetro --job-options en la instrucción de ejecución:
- sentiments: calcula los sentimientos agregados de cada comunidad/provincia de España.
- most-happy: calcula la comunidad/provincia más feliz, atendiendo a la puntuación de sentimientos anterios.
- trending: imprime los 10 trending topic que más se repiten.

## En local
python tweets_sentiments.py -r local tweets.json --file Redondo_words.txt --file comunidades.json --job-options sentiments

## Usando EMR de AWS
python tweets_sentiments.py -r emr s3://bucket/tweets.json --file s3://bucket/Redondo_words.txt
--file s3://bucket/comunidades.json --output-dir=s3://bucket/output --job-options sentiments (opciones de AWS como puede ser
tipo de máquina, región...)
