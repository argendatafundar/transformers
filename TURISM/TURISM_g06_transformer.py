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
        (row['descripcion_pais'] not in ['Estados Unidos', 'España', 
                                        'Francia', 'Italia', 'Alemania', 'Caribe', 'Resto de América',
                                        'México', 'Reino Unido']) else row['descripcion_pais'], axis=1)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	custom_logic(),
	agg_sum(key_cols=['categoria', 'sub_categoria'], summarised_col='distribucion_gasto_emisivo')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 7 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   iso2                        60 non-null     object 
#   1   iso3                        56 non-null     object 
#   2   descripcion_pais            60 non-null     object 
#   3   gasto_emisivo               60 non-null     float64
#   4   categoria                   60 non-null     object 
#   5   distribucion_gasto_emisivo  60 non-null     float64
#   6   sub_categoria               60 non-null     object 
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_emisivo | categoria    |   distribucion_gasto_emisivo | sub_categoria   |
#  |---:|:-------|:-------|:-------------------|----------------:|:-------------|-----------------------------:|:----------------|
#  |  0 | A19    |        | Resto de América   |          3120.6 | No limítrofe |                       4.8381 | Resto           |
#  
#  ------------------------------
#  
#  custom_logic()
#  RangeIndex: 60 entries, 0 to 59
#  Data columns (total 7 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   iso2                        60 non-null     object 
#   1   iso3                        56 non-null     object 
#   2   descripcion_pais            60 non-null     object 
#   3   gasto_emisivo               60 non-null     float64
#   4   categoria                   60 non-null     object 
#   5   distribucion_gasto_emisivo  60 non-null     float64
#   6   sub_categoria               60 non-null     object 
#  
#  |    | iso2   | iso3   | descripcion_pais   |   gasto_emisivo | categoria    |   distribucion_gasto_emisivo | sub_categoria    |
#  |---:|:-------|:-------|:-------------------|----------------:|:-------------|-----------------------------:|:-----------------|
#  |  0 | A19    |        | Resto de América   |          3120.6 | No limítrofe |                       4.8381 | Resto de América |
#  
#  ------------------------------
#  
#  agg_sum(key_cols=['categoria', 'sub_categoria'], summarised_col='distribucion_gasto_emisivo')
#  RangeIndex: 15 entries, 0 to 14
#  Data columns (total 3 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   categoria                   15 non-null     object 
#   1   sub_categoria               15 non-null     object 
#   2   distribucion_gasto_emisivo  15 non-null     float64
#  
#  |    | categoria   | sub_categoria   |   distribucion_gasto_emisivo |
#  |---:|:------------|:----------------|-----------------------------:|
#  |  0 | Limítrofe   | Bolivia         |                      2.96494 |
#  
#  ------------------------------
#  