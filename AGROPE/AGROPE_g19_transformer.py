from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'tipo_carne': 'indicador'}),
	drop_col(col='iso3_desc_fundar', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   iso3              1110 non-null   object 
#   1   iso3_desc_fundar  1110 non-null   object 
#   2   tipo_carne        1110 non-null   object 
#   3   valor             1107 non-null   float64
#  
#  |    | iso3   | iso3_desc_fundar   | tipo_carne       |    valor |
#  |---:|:-------|:-------------------|:-----------------|---------:|
#  |  0 | AFG    | Afganistán         | pescado_mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  rename_cols(map={'iso3': 'geocodigo', 'tipo_carne': 'indicador'})
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 4 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   geocodigo         1110 non-null   object 
#   1   iso3_desc_fundar  1110 non-null   object 
#   2   indicador         1110 non-null   object 
#   3   valor             1107 non-null   float64
#  
#  |    | geocodigo   | iso3_desc_fundar   | indicador        |    valor |
#  |---:|:------------|:-------------------|:-----------------|---------:|
#  |  0 | AFG         | Afganistán         | pescado_mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  drop_col(col='iso3_desc_fundar', axis=1)
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador        |    valor |
#  |---:|:------------|:-----------------|---------:|
#  |  0 | AFG         | pescado_mariscos | 0.359595 |
#  
#  ------------------------------
#  