from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
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
	multiplicar_por_escalar(col='valor', k=100),
	ordenar_dos_columnas(col1='region_wvs', order1=['CABA', 'Centro y Sur (exc. CABA y PBA)', 'Provincia de Buenos Aires', 'Norte'], col2='nivel_acuerdo', order2=['De acuerdo', 'Ni de acuerdo ni en desacuerdo', 'En desacuerdo'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_wvs_code  12 non-null     int64  
#   1   region_wvs       12 non-null     object 
#   2   nivel_acuerdo    12 non-null     object 
#   3   valor            12 non-null     float64
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |    valor |
#  |---:|------------------:|:-------------------------------|:----------------|---------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      | 0.128527 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 12 entries, 0 to 11
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   region_wvs_code  12 non-null     int64   
#   1   region_wvs       12 non-null     category
#   2   nivel_acuerdo    12 non-null     category
#   3   valor            12 non-null     float64 
#  
#  |    |   region_wvs_code | region_wvs                     | nivel_acuerdo   |   valor |
#  |---:|------------------:|:-------------------------------|:----------------|--------:|
#  |  0 |                 1 | Centro y Sur (exc. CABA y PBA) | De acuerdo      | 12.8527 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='region_wvs', order1=['CABA', 'Centro y Sur (exc. CABA y PBA)', 'Provincia de Buenos Aires', 'Norte'], col2='nivel_acuerdo', order2=['De acuerdo', 'Ni de acuerdo ni en desacuerdo', 'En desacuerdo'])
#  Index: 12 entries, 9 to 7
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype   
#  ---  ------           --------------  -----   
#   0   region_wvs_code  12 non-null     int64   
#   1   region_wvs       12 non-null     category
#   2   nivel_acuerdo    12 non-null     category
#   3   valor            12 non-null     float64 
#  
#  |    |   region_wvs_code | region_wvs   | nivel_acuerdo   |   valor |
#  |---:|------------------:|:-------------|:----------------|--------:|
#  |  9 |                 4 | CABA         | De acuerdo      | 3.68368 |
#  
#  ------------------------------
#  