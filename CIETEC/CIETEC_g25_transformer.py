from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
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
	replace_multiple_values(col='destino', replacements={'Erogaciones en personal': 'Personal', 'Erogaciones en inmuebles y construcciones': 'Inmuebles y construcciones', 'Erogaciones en equipamiento y rodados': 'Equipamiento y rodados', 'Otras erogaciones corrientes': 'Otras erogaciones corrientes', 'Otras erogaciones de capital': 'Otras erogaciones de capital'}),
	replace_multiple_values(col='tipo', replacements={'Nacional (total)': 'Nacional', 'Universidades públicas': 'Univ. públicas', 'Universidades privadas': 'Univ. privadas', 'Organismos públicos': 'Org. públicos', 'Empresas': 'Empresas', 'Entidades sin fines de lucro': 'Org. s/fines de lucro'}),
	ordenar_dos_columnas(col1='tipo', order1=['Nacional', 'Univ. públicas', 'Univ. privadas', 'Org. públicos', 'Empresas', 'Org. s/fines de lucro'], col2='destino', order2=['Personal', 'Inmuebles y construcciones', 'Equipamiento y rodados', 'Otras erogaciones corrientes', 'Otras erogaciones de capital'])
)
#  PIPELINE_END


#  start()
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   tipo           30 non-null     object 
#   1   anio           30 non-null     int64  
#   2   destino        30 non-null     object 
#   3   inversion_i_d  30 non-null     float64
#   4   unidad_medida  30 non-null     object 
#   5   share          30 non-null     float64
#  
#  |    | tipo     |   anio | destino                 |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:------------------------|----------------:|:-----------------------------|--------:|
#  |  0 | Empresas |   2023 | Erogaciones en personal |          255384 | millones de pesos corrientes | 52.7628 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='destino', replacements={'Erogaciones en personal': 'Personal', 'Erogaciones en inmuebles y construcciones': 'Inmuebles y construcciones', 'Erogaciones en equipamiento y rodados': 'Equipamiento y rodados', 'Otras erogaciones corrientes': 'Otras erogaciones corrientes', 'Otras erogaciones de capital': 'Otras erogaciones de capital'})
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype   
#  ---  ------         --------------  -----   
#   0   tipo           30 non-null     category
#   1   anio           30 non-null     int64   
#   2   destino        30 non-null     category
#   3   inversion_i_d  30 non-null     float64 
#   4   unidad_medida  30 non-null     object  
#   5   share          30 non-null     float64 
#  
#  |    | tipo     |   anio | destino   |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:----------|----------------:|:-----------------------------|--------:|
#  |  0 | Empresas |   2023 | Personal  |          255384 | millones de pesos corrientes | 52.7628 |
#  
#  ------------------------------
#  
#  replace_multiple_values(col='tipo', replacements={'Nacional (total)': 'Nacional', 'Universidades públicas': 'Univ. públicas', 'Universidades privadas': 'Univ. privadas', 'Organismos públicos': 'Org. públicos', 'Empresas': 'Empresas', 'Entidades sin fines de lucro': 'Org. s/fines de lucro'})
#  RangeIndex: 30 entries, 0 to 29
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype   
#  ---  ------         --------------  -----   
#   0   tipo           30 non-null     category
#   1   anio           30 non-null     int64   
#   2   destino        30 non-null     category
#   3   inversion_i_d  30 non-null     float64 
#   4   unidad_medida  30 non-null     object  
#   5   share          30 non-null     float64 
#  
#  |    | tipo     |   anio | destino   |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:----------|----------------:|:-----------------------------|--------:|
#  |  0 | Empresas |   2023 | Personal  |          255384 | millones de pesos corrientes | 52.7628 |
#  
#  ------------------------------
#  
#  ordenar_dos_columnas(col1='tipo', order1=['Nacional', 'Univ. públicas', 'Univ. privadas', 'Org. públicos', 'Empresas', 'Org. s/fines de lucro'], col2='destino', order2=['Personal', 'Inmuebles y construcciones', 'Equipamiento y rodados', 'Otras erogaciones corrientes', 'Otras erogaciones de capital'])
#  Index: 30 entries, 15 to 9
#  Data columns (total 6 columns):
#   #   Column         Non-Null Count  Dtype   
#  ---  ------         --------------  -----   
#   0   tipo           30 non-null     category
#   1   anio           30 non-null     int64   
#   2   destino        30 non-null     category
#   3   inversion_i_d  30 non-null     float64 
#   4   unidad_medida  30 non-null     object  
#   5   share          30 non-null     float64 
#  
#  |    | tipo     |   anio | destino   |   inversion_i_d | unidad_medida                |   share |
#  |---:|:---------|-------:|:----------|----------------:|:-----------------------------|--------:|
#  | 15 | Nacional |   2023 | Personal  |          766926 | millones de pesos corrientes | 66.6566 |
#  
#  ------------------------------
#  