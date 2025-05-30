from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df : DataFrame, col:str, replace_mapper:dict) -> DataFrame:
    return df.replace({col : replace_mapper})

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value'),
	replace_multiple_values(col='variable', replace_mapper={'prod_kg_pc': 'Producción', 'consumo_kg_pc': 'Consumo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 62 entries, 0 to 61
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           62 non-null     int64  
#   1   prod_kg_pc     62 non-null     float64
#   2   consumo_kg_pc  62 non-null     float64
#  
#  |    |   anio |   prod_kg_pc |   consumo_kg_pc |
#  |---:|-------:|-------------:|----------------:|
#  |  0 |   1961 |      4.91646 |         4.09159 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='value')
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      124 non-null    int64  
#   1   variable  124 non-null    object 
#   2   value     124 non-null    float64
#  
#  |    |   anio | variable   |   value |
#  |---:|-------:|:-----------|--------:|
#  |  0 |   1961 | prod_kg_pc | 4.91646 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replace_mapper={'prod_kg_pc': 'Producción', 'consumo_kg_pc': 'Consumo'})
#  RangeIndex: 124 entries, 0 to 123
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      124 non-null    int64  
#   1   variable  124 non-null    object 
#   2   value     124 non-null    float64
#  
#  |    |   anio | variable   |   value |
#  |---:|-------:|:-----------|--------:|
#  |  0 |   1961 | Producción | 4.91646 |
#  
#  ------------------------------
#  