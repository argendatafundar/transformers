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

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'iso3': 'geocodigo', 'tipo_carne': 'indicador'}),
	drop_col(col='iso3_desc_fundar', axis=1),
	replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos'),
	replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes'),
	replace_value(col='indicador', curr_value='aviar', new_value='Aviar'),
	replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina'),
	replace_value(col='indicador', curr_value='porcina', new_value='Porcina'),
	replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna')
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
#  replace_value(col='indicador', curr_value='pescado_mariscos', new_value='Pescados y mariscos')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='otras_carnes', new_value='Otras carnes')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='aviar', new_value='Aviar')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='caprina_ovina', new_value='Caprina y ovina')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='porcina', new_value='Porcina')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='vacuna', new_value='Vacuna')
#  RangeIndex: 1110 entries, 0 to 1109
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  1110 non-null   object 
#   1   indicador  1110 non-null   object 
#   2   valor      1107 non-null   float64
#  
#  |    | geocodigo   | indicador           |    valor |
#  |---:|:------------|:--------------------|---------:|
#  |  0 | AFG         | Pescados y mariscos | 0.359595 |
#  
#  ------------------------------
#  