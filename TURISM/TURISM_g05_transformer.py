from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def agg_sum(df: DataFrame, key_cols:list[str], summarised_col:str) -> DataFrame:
    return df.groupby(key_cols)[summarised_col].sum().reset_index()

@transformer.convert
def custom_logic(df: DataFrame) -> DataFrame:
    df['sub_categoria'] = df.apply(
        lambda row: 'Resto' if (row['categoria'] == 'No limítrofe') & 
        (row['descripcion_pais'] not in ['Estado Unidos', 'España', 'Perú', 
                                        'Francia', 'Italia', 'Alemania', 
                                        'Colombia', 'México', 'Reino Unido']) else row['descripcion_pais'], axis=1)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	custom_logic(),
	agg_sum(key_cols=['categoria', 'sub_categoria'], summarised_col='distribucion_gasto_receptivo')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 212 entries, 0 to 211
#  Data columns (total 7 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   iso2                          211 non-null    object 
#   1   iso3                          208 non-null    object 
#   2   descripcion_pais              212 non-null    object 
#   3   gasto_receptivo               212 non-null    float64
#   4   categoria                     212 non-null    object 
#   5   distribucion_gasto_receptivo  212 non-null    float64
#   6   sub_categoria                 212 non-null    object 
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_receptivo | categoria    |   distribucion_gasto_receptivo | sub_categoria   |
#  |---:|:-------|:-------|:-------------------|------------------:|:-------------|-------------------------------:|:----------------|
#  |  0 | A19    |        | Resto de América   |              39.5 | No limítrofe |                       0.106254 | Resto           |
#  
#  ------------------------------
#  
#  custom_logic()
#  RangeIndex: 212 entries, 0 to 211
#  Data columns (total 7 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   iso2                          211 non-null    object 
#   1   iso3                          208 non-null    object 
#   2   descripcion_pais              212 non-null    object 
#   3   gasto_receptivo               212 non-null    float64
#   4   categoria                     212 non-null    object 
#   5   distribucion_gasto_receptivo  212 non-null    float64
#   6   sub_categoria                 212 non-null    object 
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_receptivo | categoria    |   distribucion_gasto_receptivo | sub_categoria   |
#  |---:|:-------|:-------|:-------------------|------------------:|:-------------|-------------------------------:|:----------------|
#  |  0 | A19    |        | Resto de América   |              39.5 | No limítrofe |                       0.106254 | Resto           |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['categoria', 'sub_categoria'], summarised_col='distribucion_gasto_receptivo')
#  RangeIndex: 14 entries, 0 to 13
#  Data columns (total 3 columns):
#   #   Column                        Non-Null Count  Dtype  
#  ---  ------                        --------------  -----  
#   0   categoria                     14 non-null     object 
#   1   sub_categoria                 14 non-null     object 
#   2   distribucion_gasto_receptivo  14 non-null     float64
#  
#  |    | categoria   | sub_categoria   |   distribucion_gasto_receptivo |
#  |---:|:------------|:----------------|-------------------------------:|
#  |  0 | Limítrofe   | Bolivia         |                        4.36746 |
#  
#  ------------------------------
#  