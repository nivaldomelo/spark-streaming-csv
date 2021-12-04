# Spark Streaming

## Todo o processo foi feito com Apache Spark 2.4.0 e Pyspark 2.4.0. Com Python 3.8 e Java Jdk 11

## Arquivos CSVs

Deverá ser criado uma pasta que será usada para o spark ficar lendo, cada arquivo csv que cair nessa pasta o spark streaming irá ler e gravar no banco de dados postgres. Neste exemplo existem três arquivos que serão copiados em momentos diferentes para testar o streaming.

usuarios_11.csv
usuarios_12.csv
usuarios_13.csv

## Tabela usuarios no postgres

Deverá ser criada uma tabela streaming.usuarios, sendo streaming a schema, usuarios a tabela e o banco está como postgres mesmo.

CREATE TABLE streaming.usuarios (
	id text NULL,
	tipo text null,
	nome text null,
	ppu text null
);

## Jar de conexão ao postgres

O jar de conexão ao banco deverá ser guardado em uma pasta que será apontada no comando do spark-submit para conectar ao banco.

## Script Python

O script spark_streaming_csv.py fará o streaming na pasta descrita, sendo acionado a cada 5 segundos para verificar se existe um novo arquivo.
Existindo o novo arquivo o spark fará a leitura dele e guardará os dados na tabela do postgres.

# rodar spark-submit --jars /home/nivas/postgresql-9.4.1211.jar spark_streaming_csv.py
