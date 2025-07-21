from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_columns(mineral='Mineral', escenario_desarrollo_sostenible='Escenario de Desarrollo Sostenible', escenario_base='Escenario Base')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column                           Non-Null Count  Dtype  
#  ---  ------                           --------------  -----  
#   0   mineral                          9 non-null      object 
#   1   escenario_desarrollo_sostenible  9 non-null      float64
#   2   escenario_base                   9 non-null      float64
#  
#  |    | mineral   |   escenario_desarrollo_sostenible |   escenario_base |
#  |---:|:----------|----------------------------------:|-----------------:|
#  |  0 | Litio     |                              41.9 |             12.8 |
#  
#  ------------------------------
#  
#  rename_columns(mineral='Mineral', escenario_desarrollo_sostenible='Escenario de Desarrollo Sostenible', escenario_base='Escenario Base')
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column                              Non-Null Count  Dtype  
#  ---  ------                              --------------  -----  
#   0   Mineral                             9 non-null      object 
#   1   Escenario de Desarrollo Sostenible  9 non-null      float64
#   2   Escenario Base                      9 non-null      float64
#  
#  |    | Mineral   |   Escenario de Desarrollo Sostenible |   Escenario Base |
#  |---:|:----------|-------------------------------------:|-----------------:|
#  |  0 | Litio     |                                 41.9 |             12.8 |
#  
#  ------------------------------
#  