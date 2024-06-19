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
rename_cols(map={'tipo_prima': 'indicador', 'prima': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        42 non-null     int64  
#   1   tipo_prima  42 non-null     object 
#   2   prima       42 non-null     float64
#  
#  |    |   anio | tipo_prima                                  |    prima |
#  |---:|-------:|:--------------------------------------------|---------:|
#  |  0 |   2003 | Controlando por variables sociodemográficas | 0.413749 |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_prima': 'indicador', 'prima': 'valor'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   indicador  42 non-null     object 
#   2   valor      42 non-null     float64
#  
#  |    |   anio | indicador                                   |    valor |
#  |---:|-------:|:--------------------------------------------|---------:|
#  |  0 |   2003 | Controlando por variables sociodemográficas | 0.413749 |
#  
#  ------------------------------
#  