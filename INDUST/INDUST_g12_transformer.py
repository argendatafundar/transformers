from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df

@transformer.convert
def sort_values(df: DataFrame, how: str, by: list):
    if how not in ['ascending', 'descending']:
        raise ValueError('how must be either "ascending" or "descending"')
    
    return df.sort_values(by=by, ascending=how=='ascending').reset_index(drop=True)

@transformer.convert
def replace_multiple_values(df: DataFrame, col:str, replacements:dict) -> DataFrame:
    df[col] = df[col].replace(replacements)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	query(condition='anio.isin([1914, 1935, 1946, 1953, 1963, 1973, 1984, 1993, 2003, 2011, 2024])'),
	multiplicar_por_escalar(col='prop', k=100),
	replace_multiple_values(col='sector', replacements={'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Madera, papel y edición': 'Madera y papel', 'Alimentos, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, cuero y calzado': 'Textiles', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y elaborados de metal': 'Siderúrgica y metalúrgica', 'Químicos, minerales no metálicos': 'Química y minerales no metálicos', 'Otras manufacturas': 'Reparaciones y otras manufacturas', 'Maquinarias y equipos': 'Maquinaria y equipos'}),
	sort_values(how='ascending', by=['intensidad_tecnologica'])
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
#  replace_multiple_values(col='sector', replacements={'Equipos informáticos, electrónicos y eléctricos': 'Electrónica', 'Madera, papel y edición': 'Madera y papel', 'Alimentos, bebidas y tabaco': 'Alimentos, bebidas y tabaco', 'Textiles, cuero y calzado': 'Textiles', 'Equipos de transporte': 'Equipos de transporte', 'Metales básicos y elaborados de metal': 'Siderúrgica y metalúrgica', 'Químicos, minerales no metálicos': 'Química y minerales no metálicos', 'Otras manufacturas': 'Reparaciones y otras manufacturas', 'Maquinarias y equipos': 'Maquinaria y equipos'})
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
#  sort_values(how='ascending', by=['intensidad_tecnologica'])
#  RangeIndex: 99 entries, 0 to 98
#  Data columns (total 4 columns):
#   #   Column                  Non-Null Count  Dtype  
#  ---  ------                  --------------  -----  
#   0   anio                    99 non-null     int64  
#   1   sector                  99 non-null     object 
#   2   prop                    99 non-null     float64
#   3   intensidad_tecnologica  99 non-null     object 
#  
#  |    |   anio | sector               |    prop | intensidad_tecnologica   |
#  |---:|-------:|:---------------------|--------:|:-------------------------|
#  |  0 |   1973 | Maquinaria y equipos | 5.20309 | High tech                |
#  
#  ------------------------------
#  