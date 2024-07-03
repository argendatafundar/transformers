from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'salario_real_ppa_consumo_privado_2017': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1717 entries, 0 to 1716
#  Data columns (total 3 columns):
#   #   Column                                 Non-Null Count  Dtype  
#  ---  ------                                 --------------  -----  
#   0   iso3                                   1717 non-null   object 
#   1   anio                                   1717 non-null   int64  
#   2   salario_real_ppa_consumo_privado_2017  1717 non-null   float64
#  
#  |    | iso3   |   anio |   salario_real_ppa_consumo_privado_2017 |
#  |---:|:-------|-------:|----------------------------------------:|
#  |  0 | USA    |   1929 |                                 1881.73 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'salario_real_ppa_consumo_privado_2017': 'valor'})
#  RangeIndex: 1717 entries, 0 to 1716
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1717 non-null   object 
#   1   anio       1717 non-null   int64  
#   2   valor      1717 non-null   float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | USA         |   1929 | 1881.73 |
#  
#  ------------------------------
#  