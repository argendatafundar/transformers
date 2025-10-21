from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def map_categoria(df:DataFrame, curr_col:str, new_col:str, mapper:dict, default:str = None)->DataFrame:
    df[new_col] = df[curr_col].apply(lambda x: mapper.get(x, default))
    return df

@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2023])'),
	multiplicar_por_escalar(col='prop', k=100),
	map_categoria(curr_col='provincia', new_col='provincia2', mapper={'Buenos Aires': 'Buenos Aires', 'CABA': 'CABA', 'Santa Fe': 'Santa Fe', 'Córdoba': 'Córdoba', 'Mendoza': 'Mendoza', 'Tucumán': 'Tucumán'}, default='Otros'),
	agg_sum(key_cols=['anio', 'provincia2'], summarised_col='prop'),
	ordenar_dos_columnas(col1='anio', order1=[1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2023], col2='provincia2', order2=['Buenos Aires', 'CABA', 'Santa Fe', 'Córdoba', 'Mendoza', 'Tucumán', 'Otros'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 696 entries, 0 to 695
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          696 non-null    int64  
#   1   provincia_id  696 non-null    int64  
#   2   provincia     696 non-null    object 
#   3   prop          696 non-null    float64
#  
#  |    |   anio |   provincia_id | provincia   |   prop |
#  |---:|-------:|---------------:|:------------|-------:|
#  |  0 |   1913 |              2 | CABA        | 0.3719 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2023])')
#  Index: 264 entries, 0 to 695
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          264 non-null    int64  
#   1   provincia_id  264 non-null    int64  
#   2   provincia     264 non-null    object 
#   3   prop          264 non-null    float64
#   4   provincia2    264 non-null    object 
#  
#  |    |   anio |   provincia_id | provincia   |   prop | provincia2   |
#  |---:|-------:|---------------:|:------------|-------:|:-------------|
#  |  0 |   1913 |              2 | CABA        |  37.19 | CABA         |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  Index: 264 entries, 0 to 695
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          264 non-null    int64  
#   1   provincia_id  264 non-null    int64  
#   2   provincia     264 non-null    object 
#   3   prop          264 non-null    float64
#   4   provincia2    264 non-null    object 
#  
#  |    |   anio |   provincia_id | provincia   |   prop | provincia2   |
#  |---:|-------:|---------------:|:------------|-------:|:-------------|
#  |  0 |   1913 |              2 | CABA        |  37.19 | CABA         |
#  
#  ------------------------------
#  
#  map_categoria(curr_col='provincia', new_col='provincia2', mapper={'Buenos Aires': 'Buenos Aires', 'CABA': 'CABA', 'Santa Fe': 'Santa Fe', 'Córdoba': 'Córdoba', 'Mendoza': 'Mendoza', 'Tucumán': 'Tucumán'}, default='Otros')
#  Index: 264 entries, 0 to 695
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          264 non-null    int64  
#   1   provincia_id  264 non-null    int64  
#   2   provincia     264 non-null    object 
#   3   prop          264 non-null    float64
#   4   provincia2    264 non-null    object 
#  
#  |    |   anio |   provincia_id | provincia   |   prop | provincia2   |
#  |---:|-------:|---------------:|:------------|-------:|:-------------|
#  |  0 |   1913 |              2 | CABA        |  37.19 | CABA         |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['anio', 'provincia2'], summarised_col='prop')
#  RangeIndex: 77 entries, 0 to 76
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   anio        77 non-null     category
#   1   provincia2  77 non-null     category
#   2   prop        77 non-null     float64 
#  
#  |    |   anio | provincia2   |   prop |
#  |---:|-------:|:-------------|-------:|
#  |  0 |   1913 | Buenos Aires |  23.56 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1913, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2023], col2='provincia2', order2=['Buenos Aires', 'CABA', 'Santa Fe', 'Córdoba', 'Mendoza', 'Tucumán', 'Otros'])
#  Index: 77 entries, 0 to 74
#  Data columns (total 3 columns):
#   #   Column      Non-Null Count  Dtype   
#  ---  ------      --------------  -----   
#   0   anio        77 non-null     category
#   1   provincia2  77 non-null     category
#   2   prop        77 non-null     float64 
#  
#  |    |   anio | provincia2   |   prop |
#  |---:|-------:|:-------------|-------:|
#  |  0 |   1913 | Buenos Aires |  23.56 |
#  
#  ------------------------------
#  