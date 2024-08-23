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

@transformer.convert
def replace_values(df: DataFrame, col: str, values: dict):
    df = df.replace({col: values})
    return df

@transformer.convert
def sort_values_by_comparison(df, colname: str, precedence: dict, prefix=[], suffix=[]):
    mapcol = colname+'_map'
    df_ = df.copy()
    df_[mapcol] = df_[colname].map(precedence)
    df_ = df_.sort_values(by=[*prefix, mapcol, *suffix])
    return df_.drop(mapcol, axis=1)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'grupo_nuevo': 'indicador', 'impo_grupo': 'valor'}),
	multiplicar_por_escalar(col='valor', k=1e-06),
	replace_values(col='indicador', values={'aluminio': 'Aluminio', 'cinc': 'Zinc', 'ferroaleaciones': 'Ferroaleaciones', 'hierro': 'Hierro', 'otros': 'Otros'}),
	sort_values_by_comparison(colname='indicador', precedence={'Otros': 1, 'Hierro': 2, 'Aluminio': 3, 'Ferroaleaciones': 4, 'Zinc': 5}, prefix=['anio'], suffix=[])
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
#  rename_cols(map={'grupo_nuevo': 'indicador', 'impo_grupo': 'valor'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=1e-06)
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  replace_values(col='indicador', values={'aluminio': 'Aluminio', 'cinc': 'Zinc', 'ferroaleaciones': 'Ferroaleaciones', 'hierro': 'Hierro', 'otros': 'Otros'})
#  RangeIndex: 145 entries, 0 to 144
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |    | indicador   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | Aluminio    |   1994 | 8.39544 |
#  
#  ------------------------------
#  
#  sort_values_by_comparison(colname='indicador', precedence={'Otros': 1, 'Hierro': 2, 'Aluminio': 3, 'Ferroaleaciones': 4, 'Zinc': 5}, prefix=['anio'], suffix=[])
#  Index: 145 entries, 116 to 57
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  145 non-null    object 
#   1   anio       145 non-null    int64  
#   2   valor      145 non-null    float64
#  
#  |     | indicador   |   anio |   valor |
#  |----:|:------------|-------:|--------:|
#  | 116 | Otros       |   1994 | 212.459 |
#  
#  ------------------------------
#  