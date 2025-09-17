from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='valor'),
	replace_multiple_values(col='variable', replacements={'tasa_migracion_neta': 'Migración neta', 'tasa_crecimiento_vegetativo': 'Crecimiento vegetativo'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 92 entries, 0 to 91
#  Data columns (total 3 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         92 non-null     int64  
#   1   tasa_migracion_neta          92 non-null     float64
#   2   tasa_crecimiento_vegetativo  92 non-null     float64
#  
#  |    |   anio |   tasa_migracion_neta |   tasa_crecimiento_vegetativo |
#  |---:|-------:|----------------------:|------------------------------:|
#  |  0 |   1870 |               1.02304 |                       1.72043 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='valor')
#  RangeIndex: 184 entries, 0 to 183
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      184 non-null    int64  
#   1   variable  184 non-null    object 
#   2   valor     184 non-null    float64
#  
#  |    |   anio | variable       |   valor |
#  |---:|-------:|:---------------|--------:|
#  |  0 |   1870 | Migración neta | 1.02304 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'tasa_migracion_neta': 'Migración neta', 'tasa_crecimiento_vegetativo': 'Crecimiento vegetativo'})
#  RangeIndex: 184 entries, 0 to 183
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      184 non-null    int64  
#   1   variable  184 non-null    object 
#   2   valor     184 non-null    float64
#  
#  |    |   anio | variable       |   valor |
#  |---:|-------:|:---------------|--------:|
#  |  0 |   1870 | Migración neta | 1.02304 |
#  
#  ------------------------------
#  