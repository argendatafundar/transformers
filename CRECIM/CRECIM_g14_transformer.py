from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def map_values(df: DataFrame, col: str, mapping: dict):
    # Mapea los valores de la columna especificada usando el diccionario de mapeo
    df[col] = df[col].map(mapping)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def multiplicar_por_escalar(df: DataFrame, col:str, k:float):
    df[col] = df[col]*k
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
map_values(col='iso3', mapping={'CRECIM_AML-RESTO': 'Resto de América Latina', 'ARG': 'Argentina', 'BRA': 'Brasil', 'MEX': 'México'}),
	rename_cols(map={'iso3': 'categoria', 'participacion': 'valor'}),
	multiplicar_por_escalar(col='valor', k=100)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           84 non-null     int64  
#   1   iso3           84 non-null     object 
#   2   participacion  84 non-null     float64
#  
#  |    |   anio | iso3   |   participacion |
#  |---:|-------:|:-------|----------------:|
#  |  0 |   1820 | ARG    |       0.0428832 |
#  
#  ------------------------------
#  
#  map_values(col='iso3', mapping={'CRECIM_AML-RESTO': 'Resto de América Latina', 'ARG': 'Argentina', 'BRA': 'Brasil', 'MEX': 'México'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column         Non-Null Count  Dtype  
#  ---  ------         --------------  -----  
#   0   anio           84 non-null     int64  
#   1   iso3           84 non-null     object 
#   2   participacion  84 non-null     float64
#  
#  |    |   anio | iso3      |   participacion |
#  |---:|-------:|:----------|----------------:|
#  |  0 |   1820 | Argentina |       0.0428832 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'categoria', 'participacion': 'valor'})
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       84 non-null     int64  
#   1   categoria  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1820 | Argentina   | 4.28832 |
#  
#  ------------------------------
#  
#  multiplicar_por_escalar(col='valor', k=100)
#  RangeIndex: 84 entries, 0 to 83
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       84 non-null     int64  
#   1   categoria  84 non-null     object 
#   2   valor      84 non-null     float64
#  
#  |    |   anio | categoria   |   valor |
#  |---:|-------:|:------------|--------:|
#  |  0 |   1820 | Argentina   | 4.28832 |
#  
#  ------------------------------
#  