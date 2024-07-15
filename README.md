# _transformers_

El trabajo en este repositorio esta definido por un sistema de archivos que se estructura alrededor de cada uno de los tópocios de **Argendata**:


```
├── AGROPE
├── CAMCLI
├── COMEXT
├── CRECIM
├── DESHUM
├── DESIGU
├── ESTPRO
├── INFDES
├── MERTRA
├── MINERI
├── POBREZ
├── PRECIO
├── SALING
├── SEBACO
├── TRANEN
│   ├── mappings.json
│   ├── TRANEN_g01_transformer.py
│   ├── TRANEN_g02_transformer.py
│   ├── TRANEN_g03_transformer.py
│   ├── TRANEN_g04_transformer.py
│   ├── TRANEN_g05_transformer.py
│   ├── TRANEN_g06_transformer.py
│   ├── TRANEN_g07_transformer.py
│   ├── TRANEN_g08_transformer.py
│   ├── TRANEN_g09_transformer.py
│   ├── TRANEN_g10_transformer.py
│   ├── TRANEN_g11_transformer.py
│   ├── TRANEN_g12_transformer.py
│   ├── TRANEN_g13_transformer.py
│   ├── TRANEN_g14_transformer.py
│   ├── TRANEN_g15_transformer.py
│   ├── TRANEN_g16_transformer.py
│   ├── TRANEN_g17_transformer.py
│   ├── TRANEN_g18_transformer.py
│   └── TRANEN_g19_transformer.py

```
Cada dataset de Argendata tiene un nombre sustantivo y uno simplificado que funciona como id, que sigue un formato del tipo `TOPICO_g01.csv`. 

Para nuestro ejemplo, a `TRANEN_g01.csv` le corresponde [`matriz_prim_mundo_historic_larga.csv`](https://github.com/argendatafundar/data/blob/main/TRANEN/matriz_prim_mundo_historic_larga.csv). El flujo de reproducitibilidad implica en los hechos que se trata de dos _datasets_ diferentes: 

- **(i)** uno que es el resultado del proceso que generan los investigadores para luego ser reproducidos en el proceso de ETL. En nuestro ejemplo, este se realiza a partir del _script_ [`matriz_prim_mundo_historic_larga.R`](https://github.com/argendatafundar/etl/blob/main/scripts/subtopicos/TRANEN/1_matriz_prim_mundo_historic_larga.R)  en el [repositorio ETL](https://github.com/argendatafundar/etl/), facilitado por la librería [`argendataR`](https://github.com/argendatafundar/argendataR/)

- **(ii)** otro que es re transformado para darle la forma requerida por el _Frontend_ en el proceso de graficación. Esta tarea es facilitada por los _tarnsformers_. En nuestro ejemplo se trata de [`TRANEN_g01_transformer.py`](https://github.com/argendatafundar/transformers/blob/main/TRANEN/TRANEN_g01_transformer.py)

Estas equivalencias entre versiones de `.csv` están contenidas en un mapeo dentro de cada subdirectorio de topicos en el repositiorio de _transformers_: [`mappings.json`](https://github.com/argendatafundar/transformers/blob/main/TRANEN/mappings.json#L5)
