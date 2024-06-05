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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
replace_value(col='iso3', curr_value='OWID_WRL', new_value='WLD'),
	rename_cols(map={'fuente_energia': 'indicador', 'valor_en_twh': 'valor', 'iso3': 'geocodigo'})
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