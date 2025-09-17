from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='edad', replacements={3: 'Inicial', 6: 'Primaria'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype
#  ---  ------     --------------  -----
#   0   anio       148 non-null    int64
#   1   edad       148 non-null    int64
#   2   poblacion  148 non-null    int64
#  
#  |    |   anio |   edad |   poblacion |
#  |---:|-------:|-------:|------------:|
#  |  0 |   1950 |      3 |      379702 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='edad', replacements={3: 'Inicial', 6: 'Primaria'})
#  RangeIndex: 148 entries, 0 to 147
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype 
#  ---  ------     --------------  ----- 
#   0   anio       148 non-null    int64 
#   1   edad       148 non-null    object
#   2   poblacion  148 non-null    int64 
#  
#  |    |   anio | edad    |   poblacion |
#  |---:|-------:|:--------|------------:|
#  |  0 |   1950 | Inicial |      379702 |
#  
#  ------------------------------
#  