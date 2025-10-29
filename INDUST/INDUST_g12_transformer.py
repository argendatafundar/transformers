from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def ordenar_dos_columnas(df, col1:str, order1:list[str], col2:str, order2:list[str]):
    import pandas as pd
    df[col1] = pd.Categorical(df[col1], categories=order1, ordered=True)
    df[col2] = pd.Categorical(df[col2], categories=order2, ordered=True)
    return df.sort_values(by=[col1,col2])

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict, new_col:str = None) -> DataFrame:
    new_col = col if new_col is None else new_col
    df_copy = df.copy()
    df_copy[new_col] = df_copy[col].replace(replacements)
    return df_copy

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([1914, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024])'),
	multiplicar_por_escalar(col='prop', k=100),
	replace_multiple_values(col='sector', replacements={'Alimentos, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, cuero y calzado': 'Textiles y calzado', 'Otras manufacturas': 'Otras manufacturas', 'Madera, papel y edición': 'Madera y papel', 'Químicos, minerales no metálicos': 'Química y minerales no metálicos', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y elaborados de metal': 'Siderúrgica y metalúrgica', 'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Maquinarias y equipos': 'Maquinaria y equipos'}, new_col=None),
	ordenar_dos_columnas(col1='anio', order1=[1914, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024], col2='sector', order2=['Alimentos, bebidas y tabaco', 'Textiles y calzado', 'Madera y papel', 'Otras manufacturas', 'Química y minerales no metálicos', 'Equipos de transporte', 'Siderúrgica y metalúrgica', 'Electrónica', 'Maquinaria y equipos'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 270 entries, 0 to 269
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    270 non-null    int64  
#   1   sector                  270 non-null    object 
#   2   prop                    270 non-null    float64
#   3   intensidad_tecnologica  270 non-null    object 
#  
#  |    |   anio | sector                      |     prop | intensidad_tecnologica   |
#  |---:|-------:|:----------------------------|---------:|:-------------------------|
#  |  0 |   1914 | Alimentos, bebidas y tabaco | 0.505876 | Low tech                 |
#  
#  ------------------------------
#  
#  query(condition='anio.isin([1914, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024])')
#  Index: 99 entries, 0 to 269
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    99 non-null     int64  
#   1   sector                  99 non-null     object 
#   2   prop                    99 non-null     float64
#   3   intensidad_tecnologica  99 non-null     object 
#  
#  |    |   anio | sector                      |    prop | intensidad_tecnologica   |
#  |---:|-------:|:----------------------------|--------:|:-------------------------|
#  |  0 |   1914 | Alimentos, bebidas y tabaco | 50.5876 | Low tech                 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='prop', k=100)
#  Index: 99 entries, 0 to 269
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    99 non-null     int64  
#   1   sector                  99 non-null     object 
#   2   prop                    99 non-null     float64
#   3   intensidad_tecnologica  99 non-null     object 
#  
#  |    |   anio | sector                      |    prop | intensidad_tecnologica   |
#  |---:|-------:|:----------------------------|--------:|:-------------------------|
#  |  0 |   1914 | Alimentos, bebidas y tabaco | 50.5876 | Low tech                 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='sector', replacements={'Alimentos, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, cuero y calzado': 'Textiles y calzado', 'Otras manufacturas': 'Otras manufacturas', 'Madera, papel y edición': 'Madera y papel', 'Químicos, minerales no metálicos': 'Química y minerales no metálicos', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y elaborados de metal': 'Siderúrgica y metalúrgica', 'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Maquinarias y equipos': 'Maquinaria y equipos'}, new_col=None)
#  Index: 99 entries, 0 to 269
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype   
#  ---  ------                  --------------  -----   
#   0   anio                    99 non-null     category
#   1   sector                  99 non-null     category
#   2   prop                    99 non-null     float64 
#   3   intensidad_tecnologica  99 non-null     object  
#  
#  |    |   anio | sector                      |    prop | intensidad_tecnologica   |
#  |---:|-------:|:----------------------------|--------:|:-------------------------|
#  |  0 |   1914 | Alimentos, bebidas y tabaco | 50.5876 | Low tech                 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='anio', order1=[1914, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024], col2='sector', order2=['Alimentos, bebidas y tabaco', 'Textiles y calzado', 'Madera y papel', 'Otras manufacturas', 'Química y minerales no metálicos', 'Equipos de transporte', 'Siderúrgica y metalúrgica', 'Electrónica', 'Maquinaria y equipos'])
#  Index: 99 entries, 0 to 265
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype   
#  ---  ------                  --------------  -----   
#   0   anio                    99 non-null     category
#   1   sector                  99 non-null     category
#   2   prop                    99 non-null     float64 
#   3   intensidad_tecnologica  99 non-null     object  
#  
#  |    |   anio | sector                      |    prop | intensidad_tecnologica   |
#  |---:|-------:|:----------------------------|--------:|:-------------------------|
#  |  0 |   1914 | Alimentos, bebidas y tabaco | 50.5876 | Low tech                 |
#  
#  ------------------------------
#  