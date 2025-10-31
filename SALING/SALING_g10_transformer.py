from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def concatenar_columnas(df:DataFrame, cols:list, nueva_col:str, separtor:str = "-"):
    df[nueva_col] = df[cols].astype(str).agg(separtor.join, axis=1)
    return df

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df_copy = df.copy()
    df_copy[col] = df_copy[col].replace(replacements)
    return df_copy
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	replace_multiple_values(col='fuente', replacements={'Ingreso laboral': 'Laboral', 'Ingreso de capital': 'Capital', 'Ingreso de jubilaciones': 'Jubilaciones', 'Ingreso de transferencias estatales': 'Transferencias estatales', 'Otros ingresos': 'Otros ingresos'}),
	concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 4 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      168 non-null    int64  
#   1   semestre  168 non-null    int64  
#   2   fuente    168 non-null    object 
#   3   indice    160 non-null    float64
#  
#  |    |   year |   semestre | fuente             |   indice |
#  |---:|-------:|-----------:|:-------------------|---------:|
#  |  0 |   2003 |          2 | Ingreso de capital |      100 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='fuente', replacements={'Ingreso laboral': 'Laboral', 'Ingreso de capital': 'Capital', 'Ingreso de jubilaciones': 'Jubilaciones', 'Ingreso de transferencias estatales': 'Transferencias estatales', 'Otros ingresos': 'Otros ingresos'})
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      168 non-null    int64  
#   1   semestre  168 non-null    int64  
#   2   fuente    168 non-null    object 
#   3   indice    160 non-null    float64
#   4   aniosem   168 non-null    object 
#  
#  |    |   year |   semestre | fuente   |   indice | aniosem   |
#  |---:|-------:|-----------:|:---------|---------:|:----------|
#  |  0 |   2003 |          2 | Capital  |      100 | 2003-2    |
#  
#  ------------------------------
#  
#  concatenar_columnas(cols=['year', 'semestre'], nueva_col='aniosem', separtor='-')
#  RangeIndex: 168 entries, 0 to 167
#  Data columns (total 5 columns):
#   #   Column    Non-Null Count  Dtype  
#  ---  ------    --------------  -----  
#   0   year      168 non-null    int64  
#   1   semestre  168 non-null    int64  
#   2   fuente    168 non-null    object 
#   3   indice    160 non-null    float64
#   4   aniosem   168 non-null    object 
#  
#  |    |   year |   semestre | fuente   |   indice | aniosem   |
#  |---:|-------:|-----------:|:---------|---------:|:----------|
#  |  0 |   2003 |          2 | Capital  |      100 | 2003-2    |
#  
#  ------------------------------
#  