from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def identity(df: DataFrame) -> DataFrame:
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	identity()
)
#  PIPELINE_END


#  start()
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         151 non-null    object 
#   1   geonombreFundar         151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#   3   fuente                  151 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   gasto_publico_promedio | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|
#  |  0 | KIR               | Kiribati          |                  97.4885 | FMI      |
#  
#  ------------------------------
#  
#  identity()
#  RangeIndex: 151 entries, 0 to 150
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   geocodigoFundar         151 non-null    object 
#   1   geonombreFundar         151 non-null    object 
#   2   gasto_publico_promedio  151 non-null    float64
#   3   fuente                  151 non-null    object 
#  
#  |    | geocodigoFundar   | geonombreFundar   |   gasto_publico_promedio | fuente   |
#  |---:|:------------------|:------------------|-------------------------:|:---------|
#  |  0 | KIR               | Kiribati          |                  97.4885 | FMI      |
#  
#  ------------------------------
#  