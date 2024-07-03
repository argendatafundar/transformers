from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def mutiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'serie': 'indicador', 'impo_grupo': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=1e-06)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  145 non-null    object 
#   1   anio         145 non-null    int64  
#   2   impo_grupo   145 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   impo_grupo |
#  |---:|:--------------|-------:|-------------:|
#  |  0 | aluminio      |   1994 |  8.39544e+06 |
#  
#  ------------------------------
#  
#  rename_cols(map={'serie': 'indicador', 'impo_grupo': 'valor'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  145 non-null    object 
#   1   anio         145 non-null    int64  
#   2   valor        145 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | aluminio      |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column       Non-Null Count  Dtype  
#  ---  ------       --------------  -----  
#   0   grupo_nuevo  145 non-null    object 
#   1   anio         145 non-null    int64  
#   2   valor        145 non-null    float64
#  
#  |    | grupo_nuevo   |   anio |   valor |
#  |---:|:--------------|-------:|--------:|
#  |  0 | aluminio      |   1994 | 8.39544 |
#  
#  ------------------------------
#  