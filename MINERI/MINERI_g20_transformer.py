from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def rename_columns(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_columns(mineral='Mineral', escenario_desarrollo_sostenible='Desarrollo Sostenible', escenario_base='Base'),
	pivot_longer(id_cols='Mineral', names_to_col='escenarios', values_to_col='valor')
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
#  rename_columns(mineral='Mineral', escenario_desarrollo_sostenible='Desarrollo Sostenible', escenario_base='Base')
#  RangeIndex: 9 entries, 0 to 8
#  Data columns (total 3 columns):
#   #   Column                 Non-Null Count  Dtype  
#  ---  ------                 --------------  -----  
#   0   Mineral                9 non-null      object 
#   1   Desarrollo Sostenible  9 non-null      float64
#   2   Base                   9 non-null      float64
#  
#  |    | Mineral   |   Desarrollo Sostenible |   Base |
#  |---:|:----------|------------------------:|-------:|
#  |  0 | Litio     |                    41.9 |   12.8 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols='Mineral', names_to_col='escenarios', values_to_col='valor')
#  RangeIndex: 18 entries, 0 to 17
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   Mineral     18 non-null     object 
#   1   escenarios  18 non-null     object 
#   2   valor       18 non-null     float64
#  
#  |    | Mineral   | escenarios            |   valor |
#  |---:|:----------|:----------------------|--------:|
#  |  0 | Litio     | Desarrollo Sostenible |    41.9 |
#  
#  ------------------------------
#  