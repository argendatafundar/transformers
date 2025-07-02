from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_columns_(df: DataFrame, **kwargs):
    df = df.rename(columns=kwargs)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_columns_(grupo_de_paises='categoria', mediana_paises_inflacion='valor'),
	replace_value(col='categoria', curr_value='Países de Altos Ingresos', new_value='Países de altos ingresos')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   grupo_de_paises           88 non-null     object 
#   1   anio                      88 non-null     int64  
#   2   mediana_paises_inflacion  88 non-null     float64
#  
#  |    | grupo_de_paises         |   anio |   mediana_paises_inflacion |
#  |---:|:------------------------|-------:|---------------------------:|
#  |  0 | América Latina y Caribe |   1980 |                     21.206 |
#  
#  ------------------------------
#  
#  rename_columns_(grupo_de_paises='categoria', mediana_paises_inflacion='valor')
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  88 non-null     object 
#   1   anio       88 non-null     int64  
#   2   valor      88 non-null     float64
#  
#  |    | categoria               |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | América Latina y Caribe |   1980 |  21.206 |
#  
#  ------------------------------
#  
#  replace_value(col='categoria', curr_value='Países de Altos Ingresos', new_value='Países de altos ingresos')
#  RangeIndex: 88 entries, 0 to 87
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   categoria  88 non-null     object 
#   1   anio       88 non-null     int64  
#   2   valor      88 non-null     float64
#  
#  |    | categoria               |   anio |   valor |
#  |---:|:------------------------|-------:|--------:|
#  |  0 | América Latina y Caribe |   1980 |  21.206 |
#  
#  ------------------------------
#  