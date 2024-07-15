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
rename_cols(map={'provincia': 'categoria', 'nivel_ed_fundar': 'indicador', 'ocupado': 'valor'}),
	mutiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             756 non-null    int64  
#   1   provincia        756 non-null    object 
#   2   nivel_ed_fundar  756 non-null    object 
#   3   ocupado          756 non-null    float64
#  
#  |    |   anio | provincia    | nivel_ed_fundar             |   ocupado |
#  |---:|-------:|:-------------|:----------------------------|----------:|
#  |  0 |   2016 | Buenos Aires | Hasta secundario incompleto |  0.647369 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'categoria', 'nivel_ed_fundar': 'indicador', 'ocupado': 'valor'})
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       756 non-null    int64  
#   1   categoria  756 non-null    object 
#   2   indicador  756 non-null    object 
#   3   valor      756 non-null    float64
#  
#  |    |   anio | categoria    | indicador                   |   valor |
#  |---:|-------:|:-------------|:----------------------------|--------:|
#  |  0 |   2016 | Buenos Aires | Hasta secundario incompleto | 64.7369 |
#  
#  ------------------------------
#  
#  mutiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 756 entries, 0 to 755
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       756 non-null    int64  
#   1   categoria  756 non-null    object 
#   2   indicador  756 non-null    object 
#   3   valor      756 non-null    float64
#  
#  |    |   anio | categoria    | indicador                   |   valor |
#  |---:|-------:|:-------------|:----------------------------|--------:|
#  |  0 |   2016 | Buenos Aires | Hasta secundario incompleto | 64.7369 |
#  
#  ------------------------------
#  