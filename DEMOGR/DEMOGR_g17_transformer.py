from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def pivot_longer(df: DataFrame, id_cols:list[str], names_to_col:str, values_to_col:str) -> DataFrame:
    return df.melt(id_vars=id_cols, var_name=names_to_col, value_name=values_to_col)

@transformer.convert
def sumar_dos_columnas(df:DataFrame, col1:str, col2:str, new_col:str)->DataFrame:
    df[new_col] = df[col1] + df[col2]
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	sumar_dos_columnas(col1='tasa_migracion_neta', col2='tasa_crecimiento_vegetativo', new_col='tasa_crecimiento_total'),
	pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='valor'),
	replace_multiple_values(col='variable', replacements={'tasa_migracion_neta': 'Migración neta', 'tasa_crecimiento_vegetativo': 'Crecimiento vegetativo', 'tasa_crecimiento_total': 'Crecimiento total'})
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
#  sumar_dos_columnas(col1='tasa_migracion_neta', col2='tasa_crecimiento_vegetativo', new_col='tasa_crecimiento_total')
#  RangeIndex: 92 entries, 0 to 91
#  Data columns (total 4 columns):
#   #   Column                       Non-Null Count  Dtype  
#  ---  ------                       --------------  -----  
#   0   anio                         92 non-null     int64  
#   1   tasa_migracion_neta          92 non-null     float64
#   2   tasa_crecimiento_vegetativo  92 non-null     float64
#   3   tasa_crecimiento_total       92 non-null     float64
#  
#  |    |   anio |   tasa_migracion_neta |   tasa_crecimiento_vegetativo |   tasa_crecimiento_total |
#  |---:|-------:|----------------------:|------------------------------:|-------------------------:|
#  |  0 |   1870 |               1.02304 |                       1.72043 |                  2.74347 |
#  
#  ------------------------------
#  
#  pivot_longer(id_cols=['anio'], names_to_col='variable', values_to_col='valor')
#  RangeIndex: 276 entries, 0 to 275
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      276 non-null    int64  
#   1   variable  276 non-null    object 
#   2   valor     276 non-null    float64
#  
#  |    |   anio | variable            |   valor |
#  |---:|-------:|:--------------------|--------:|
#  |  0 |   1870 | tasa_migracion_neta | 1.02304 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='variable', replacements={'tasa_migracion_neta': 'Migración neta', 'tasa_crecimiento_vegetativo': 'Crecimiento vegetativo', 'tasa_crecimiento_total': 'Crecimiento total'})
#  RangeIndex: 276 entries, 0 to 275
#  Data columns (total 3 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   anio      276 non-null    int64  
#   1   variable  276 non-null    object 
#   2   valor     276 non-null    float64
#  
#  |    |   anio | variable       |   valor |
#  |---:|-------:|:---------------|--------:|
#  |  0 |   1870 | Migración neta | 1.02304 |
#  
#  ------------------------------
#  