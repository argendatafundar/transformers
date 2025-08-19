from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio >= 1980')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 111 entries, 0 to 110
#  Data columns (total 2 columns):
#   #   Column                                 Non-Null Count  Dtype  
#  ---  ------                                 --------------  -----  
#   0   anio                                   111 non-null    int64  
#   1   gasto_publico_consolidado_pib_empalme  111 non-null    float64
#  
#  |    |   anio |   gasto_publico_consolidado_pib_empalme |
#  |---:|-------:|----------------------------------------:|
#  |  0 |   1913 |                                   12.21 |
#  
#  ------------------------------
#  
#  query(condition='anio >= 1980')
#  Index: 44 entries, 67 to 110
#  Data columns (total 2 columns):
#   #   Column                                 Non-Null Count  Dtype  
#  ---  ------                                 --------------  -----  
#   0   anio                                   44 non-null     int64  
#   1   gasto_publico_consolidado_pib_empalme  44 non-null     float64
#  
#  |    |   anio |   gasto_publico_consolidado_pib_empalme |
#  |---:|-------:|----------------------------------------:|
#  | 67 |   1980 |                                 29.0371 |
#  
#  ------------------------------
#  