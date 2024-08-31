<a href="https://fund.ar">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/1644e28e-63a8-4b51-b6a1-fa68a30fa9c2">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/1d1d2193-8680-4a71-9b71-4832caefadd8">
    <img src="fund.ar" width="400"></img>
  </picture>
</a>

Un _transformer_ es un _script_ en `Python` ejecutable, auto-contenido y auto-reproducible. Esencialmente son "recetas" fácilmente aplicables a un dataset,
compuesto por una serie de instrucciones (funciones) para mutarlo, junto con las definiciones exactas de cada una de ellas. De esta manera, cada archivo
describe en sí mismo su comportamiento, siendo así expresivo respecto de su funcionalidad, pero también fácilmente ejecutable en un proceso de automatización.

[(Ver _data-transformers_)](https://github.com/argendatafundar/data-transformers)

Cada uno de estos archivos tiene como objetivo generar `CSVs` listos para ser graficados por el [sitio web de ArgenData](https://argendata.fund.ar), por lo que
implica un proceso de normalización más fuerte que la armonización previa [_etl_](https://github.com/argendatafundar/etl). Junto con la modificación estructural del _dataset_, se deja sólo lo imprescindible para
que el gráfico pueda ser visualizado, generando así archivos mucho más livianos.

## Esquema del sistema de archivos
El trabajo en este repositorio esta definido por un sistema de archivos que se estructura alrededor de cada uno de los tópicos de **Argendata**
```
├── TOPICO
├── ...
├── AGROPE
│   ├── mappings.json
│   ├── TOPICO_gXX_transformer.py
│   └── ...
└── TRANEN
    ├── mappings.json
    ├── TRANEN_g01_transformer.py
    ├── TRANEN_g02_transformer.py
    ├── TRANEN_g03_transformer.py
    ├── TRANEN_g04_transformer.py
    ├── TRANEN_g05_transformer.py
    ├── TRANEN_g06_transformer.py
    ├── TRANEN_g07_transformer.py
    ├── TRANEN_g08_transformer.py
    ├── TRANEN_g09_transformer.py
    ├── TRANEN_g10_transformer.py
    ├── TRANEN_g11_transformer.py
    ├── TRANEN_g12_transformer.py
    ├── TRANEN_g13_transformer.py
    ├── TRANEN_g14_transformer.py
    ├── TRANEN_g15_transformer.py
    ├── TRANEN_g16_transformer.py
    ├── TRANEN_g17_transformer.py
    ├── TRANEN_g18_transformer.py
    └── TRANEN_g19_transformer.py
```
Cada dataset de Argendata tiene un nombre sustantivo y uno simplificado que funciona como _id_, que sigue un formato del tipo `TOPICO_g01.csv`. 

Para nuestro ejemplo, a `TRANEN_g01.csv` le corresponde [`matriz_prim_mundo_historic_larga.csv`](https://github.com/argendatafundar/data/blob/main/TRANEN/matriz_prim_mundo_historic_larga.csv). El flujo de reproducitibilidad implica en los hechos que se trata de dos _datasets_ diferentes: 

- **(i)** uno que es el resultado del proceso que generan los investigadores para luego ser reproducidos en el proceso de _ETL_. En nuestro ejemplo, este se realiza a partir del _script_ [`matriz_prim_mundo_historic_larga.R`](https://github.com/argendatafundar/etl/blob/main/scripts/subtopicos/TRANEN/1_matriz_prim_mundo_historic_larga.R) en el [repositorio ETL](https://github.com/argendatafundar/etl/), facilitado por la librería de `R` [`argendataR`](https://github.com/argendatafundar/argendataR/)

- **(ii)** otro que es re transformado para darle la forma requerida por el _Frontend_ en el proceso de graficación. Esta tarea es facilitada por los _tarnsformers_. En nuestro ejemplo se trata de [`TRANEN_g01_transformer.py`](https://github.com/argendatafundar/transformers/blob/main/TRANEN/TRANEN_g01_transformer.py) facilitado por la biblioteca de `Python` [`data-transformers`](https://github.com/argendatafundar/data-transformers)

Estas equivalencias entre versiones de `.csv` están contenidas en un mapeo dentro de cada subdirectorio de topicos en el repositiorio de _transformers_: [`mappings.json`](https://github.com/argendatafundar/transformers/blob/main/TRANEN/mappings.json#L5)



## `TRANEN_g01_transformer.py`

``` python
from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    return df.sort_values(by=by, ascending=how == 'ascending')
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
query(condition='tipo_energia != "Total"'),
	rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'}),
	sort_values(how='ascending', by=['anio', 'indicador'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 836 entries, 0 to 835
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          836 non-null    int64  
#   1   tipo_energia  836 non-null    object 
#   2   valor_en_twh  836 non-null    float64
#   3   porcentaje    836 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  query(condition='tipo_energia != "Total"')
#  Index: 760 entries, 0 to 759
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          760 non-null    int64  
#   1   tipo_energia  760 non-null    object 
#   2   valor_en_twh  760 non-null    float64
#   3   porcentaje    760 non-null    float64
#  
#  |    |   anio | tipo_energia     |   valor_en_twh |   porcentaje |
#  |---:|-------:|:-----------------|---------------:|-------------:|
#  |  0 |   1800 | Otras renovables |              0 |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_energia': 'indicador', 'valor_en_twh': 'valor'})
#  Index: 760 entries, 0 to 759
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        760 non-null    int64  
#   1   indicador   760 non-null    object 
#   2   valor       760 non-null    float64
#   3   porcentaje  760 non-null    float64
#  
#  |    |   anio | indicador        |   valor |   porcentaje |
#  |---:|-------:|:-----------------|--------:|-------------:|
#  |  0 |   1800 | Otras renovables |       0 |            0 |
#  
#  ------------------------------
#  
#  sort_values(how='ascending', by=['anio', 'indicador'])
#  Index: 760 entries, 76 to 227
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        760 non-null    int64  
#   1   indicador   760 non-null    object 
#   2   valor       760 non-null    float64
#   3   porcentaje  760 non-null    float64
#  
#  |    |   anio | indicador       |   valor |   porcentaje |
#  |---:|-------:|:----------------|--------:|-------------:|
#  | 76 |   1800 | Biocombustibles |       0 |            0 |
#  
#  ------------------------------
#  

```
<div>
<a href="https://fund.ar">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/datos-Fundar/fundartools/assets/86327859/6ef27bf9-141f-4537-9d78-e16b80196959">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/datos-Fundar/fundartools/assets/86327859/aa8e7c72-4fad-403a-a8b9-739724b4c533">
    <img src="fund.ar"></img>
  </picture>
</a>
</div>

