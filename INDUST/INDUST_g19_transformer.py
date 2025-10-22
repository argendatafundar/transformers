from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition="intensidad_id_ocde_desc == 'Media y alta intensidad de I+D'"),
	multiplicar_por_escalar(col='prop_vab', k=100),
	query(condition='anio.isin([1995, 2000, 2004, 2011, 2022])')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 4984 entries, 0 to 4983
#  Data columns (total 6 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     4984 non-null   int64  
#   1   geocodigoFundar          4984 non-null   object 
#   2   geonombreFundar          4984 non-null   object 
#   3   intensidad_id_ocde_desc  4984 non-null   object 
#   4   vab                      4984 non-null   float64
#   5   prop_vab                 4984 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde_desc   |     vab |   prop_vab |
#  |---:|-------:|:------------------|:------------------|:--------------------------|--------:|-----------:|
#  |  0 |   1995 | AGO               | Angola            | Baja intensidad de I+D    | 201.503 |   0.517627 |
#  
#  ------------------------------
#  
#  query(condition="intensidad_id_ocde_desc == 'Media y alta intensidad de I+D'")
#  Index: 2492 entries, 1 to 4983
#  Data columns (total 6 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     2492 non-null   int64  
#   1   geocodigoFundar          2492 non-null   object 
#   2   geonombreFundar          2492 non-null   object 
#   3   intensidad_id_ocde_desc  2492 non-null   object 
#   4   vab                      2492 non-null   float64
#   5   prop_vab                 2492 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde_desc        |     vab |   prop_vab |
#  |---:|-------:|:------------------|:------------------|:-------------------------------|--------:|-----------:|
#  |  1 |   1995 | AGO               | Angola            | Media y alta intensidad de I+D | 187.779 |    48.2373 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop_vab', k=100)
#  Index: 2492 entries, 1 to 4983
#  Data columns (total 6 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     2492 non-null   int64  
#   1   geocodigoFundar          2492 non-null   object 
#   2   geonombreFundar          2492 non-null   object 
#   3   intensidad_id_ocde_desc  2492 non-null   object 
#   4   vab                      2492 non-null   float64
#   5   prop_vab                 2492 non-null   float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde_desc        |     vab |   prop_vab |
#  |---:|-------:|:------------------|:------------------|:-------------------------------|--------:|-----------:|
#  |  1 |   1995 | AGO               | Angola            | Media y alta intensidad de I+D | 187.779 |    48.2373 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1995, 2000, 2004, 2011, 2022])')
#  Index: 445 entries, 1 to 4983
#  Data columns (total 6 columns):
#   #   Column                   Non-Null Count  Dtype  
#  ---  ------                   --------------  -----  
#   0   anio                     445 non-null    int64  
#   1   geocodigoFundar          445 non-null    object 
#   2   geonombreFundar          445 non-null    object 
#   3   intensidad_id_ocde_desc  445 non-null    object 
#   4   vab                      445 non-null    float64
#   5   prop_vab                 445 non-null    float64
#  
#  |    |   anio | geocodigoFundar   | geonombreFundar   | intensidad_id_ocde_desc        |     vab |   prop_vab |
#  |---:|-------:|:------------------|:------------------|:-------------------------------|--------:|-----------:|
#  |  1 |   1995 | AGO               | Angola            | Media y alta intensidad de I+D | 187.779 |    48.2373 |
#  
#  ------------------------------
#  