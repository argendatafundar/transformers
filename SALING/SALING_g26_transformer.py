from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'tipo_prima': 'categoria', 'prima': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   anio        42 non-null     int64  
#   1   prima       42 non-null     float64
#   2   tipo_prima  42 non-null     object 
#  
#  |    |   anio |    prima | tipo_prima                                  |
#  |---:|-------:|---------:|:--------------------------------------------|
#  |  0 |   2003 | 0.438929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  
#  rename_cols(map={'tipo_prima': 'categoria', 'prima': 'valor'})
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   valor      42 non-null     float64
#   2   categoria  42 non-null     object 
#  
#  |    |   anio |   valor | categoria                                   |
#  |---:|-------:|--------:|:--------------------------------------------|
#  |  0 |   2003 | 43.8929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 42 entries, 0 to 41
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       42 non-null     int64  
#   1   valor      42 non-null     float64
#   2   categoria  42 non-null     object 
#  
#  |    |   anio |   valor | categoria                                   |
#  |---:|-------:|--------:|:--------------------------------------------|
#  |  0 |   2003 | 43.8929 | Controlando por variables sociodemográficas |
#  
#  ------------------------------
#  