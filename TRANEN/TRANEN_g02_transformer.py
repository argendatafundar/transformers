from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def replace_value(df: DataFrame, col: str, curr_value: str, new_value: str):
    df = df.replace({col: curr_value}, new_value)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
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
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'}),
	query(condition='indicador != "Total"'),
	replace_value(col='indicador', curr_value='Carbon', new_value='Carbón'),
	replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo'),
	replace_value(col='indicador', curr_value='Eolica', new_value='Eólica'),
	replace_value(col='indicador', curr_value='Biomasa tradicional ', new_value='Biomasa tradicional'),
	drop_col(col='porcentaje', axis=1),
	drop_col(col='tipo_energia', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 51852 entries, 0 to 51851
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            51852 non-null  object 
#   1   anio            51852 non-null  int64  
#   2   fuente_energia  51852 non-null  object 
#   3   tipo_energia    51852 non-null  object 
#   4   valor_en_twh    40880 non-null  float64
#   5   porcentaje      51852 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD')
#  RangeIndex: 51852 entries, 0 to 51851
#  Data columns (total 6 columns):
#   #   Column          Non-Null Count  Dtype  
#  ---  ------          --------------  -----  
#   0   iso3            51852 non-null  object 
#   1   anio            51852 non-null  int64  
#   2   fuente_energia  51852 non-null  object 
#   3   tipo_energia    51852 non-null  object 
#   4   valor_en_twh    40880 non-null  float64
#   5   porcentaje      51852 non-null  float64
#  
#  |    | iso3   |   anio | fuente_energia   | tipo_energia   |   valor_en_twh |   porcentaje |
#  |---:|:-------|-------:|:-----------------|:---------------|---------------:|-------------:|
#  |  0 | AGO    |   1965 | Biocombustibles  | Limpias        |            nan |            0 |
#  
#  ------------------------------
#  
#  rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
#  RangeIndex: 51852 entries, 0 to 51851
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     51852 non-null  object 
#   1   anio          51852 non-null  int64  
#   2   indicador     51852 non-null  object 
#   3   tipo_energia  51852 non-null  object 
#   4   valor         40880 non-null  float64
#   5   porcentaje    51852 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  query(condition='indicador != "Total"')
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#   5   porcentaje    45530 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Carbon', new_value='Carbón')
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#   5   porcentaje    45530 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Petroleo', new_value='Petróleo')
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#   5   porcentaje    45530 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Eolica', new_value='Eólica')
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#   5   porcentaje    45530 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='Biomasa tradicional ', new_value='Biomasa tradicional')
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 6 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#   5   porcentaje    45530 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |   porcentaje |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|-------------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |            0 |
#  
#  ------------------------------
#  
#  drop_col(col='porcentaje', axis=1)
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 5 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     45530 non-null  object 
#   1   anio          45530 non-null  int64  
#   2   indicador     45530 non-null  object 
#   3   tipo_energia  45530 non-null  object 
#   4   valor         34558 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       | tipo_energia   |   valor |
#  |---:|:------------|-------:|:----------------|:---------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles | Limpias        |     nan |
#  
#  ------------------------------
#  
#  drop_col(col='tipo_energia', axis=1)
#  Index: 45530 entries, 0 to 51793
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  45530 non-null  object 
#   1   anio       45530 non-null  int64  
#   2   indicador  45530 non-null  object 
#   3   valor      34558 non-null  float64
#  
#  |    | geocodigo   |   anio | indicador       |   valor |
#  |---:|:------------|-------:|:----------------|--------:|
#  |  0 | AGO         |   1965 | Biocombustibles |     nan |
#  
#  ------------------------------
#  