from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def convert_indec_codes_to_isoprov(df: DataFrame, df_cod_col:str):
   
   from io import StringIO
   from pandas.api.types import is_string_dtype, is_numeric_dtype
   import pandas as pd

   json_str = '[{"codprov_str":"02","codprov_iso":"AR-C","codprov_int":2},{"codprov_str":"06","codprov_iso":"AR-B","codprov_int":6},{"codprov_str":"10","codprov_iso":"AR-K","codprov_int":10},{"codprov_str":"14","codprov_iso":"AR-X","codprov_int":14},{"codprov_str":"18","codprov_iso":"AR-W","codprov_int":18},{"codprov_str":"22","codprov_iso":"AR-H","codprov_int":22},{"codprov_str":"26","codprov_iso":"AR-U","codprov_int":26},{"codprov_str":"30","codprov_iso":"AR-E","codprov_int":30},{"codprov_str":"34","codprov_iso":"AR-P","codprov_int":34},{"codprov_str":"38","codprov_iso":"AR-Y","codprov_int":38},{"codprov_str":"42","codprov_iso":"AR-L","codprov_int":42},{"codprov_str":"46","codprov_iso":"AR-F","codprov_int":46},{"codprov_str":"50","codprov_iso":"AR-M","codprov_int":50},{"codprov_str":"54","codprov_iso":"AR-N","codprov_int":54},{"codprov_str":"58","codprov_iso":"AR-Q","codprov_int":58},{"codprov_str":"62","codprov_iso":"AR-R","codprov_int":62},{"codprov_str":"66","codprov_iso":"AR-A","codprov_int":66},{"codprov_str":"70","codprov_iso":"AR-J","codprov_int":70},{"codprov_str":"74","codprov_iso":"AR-D","codprov_int":74},{"codprov_str":"78","codprov_iso":"AR-Z","codprov_int":78},{"codprov_str":"82","codprov_iso":"AR-S","codprov_int":82},{"codprov_str":"86","codprov_iso":"AR-G","codprov_int":86},{"codprov_str":"90","codprov_iso":"AR-T","codprov_int":90},{"codprov_str":"94","codprov_iso":"AR-V","codprov_int":94}]'
   diccionario : DataFrame = pd.read_json(StringIO(json_str), dtype={'codprov_str':str, 'codprov_iso':str, 'codprov_int':int})

   col_used = 'codprov_str'
   if is_numeric_dtype(df[df_cod_col]):
      col_used = 'codprov_int'
      
   replace_mapper = diccionario.set_index(col_used)['codprov_iso'].to_dict()
   df[df_cod_col] = df[df_cod_col].replace(replace_mapper)

   return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def wide_to_long(df: DataFrame, primary_keys, value_name='valor', var_name='indicador'):
    return df.melt(id_vars=primary_keys, value_name=value_name, var_name=var_name)

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
convert_indec_codes_to_isoprov(df_cod_col='provincia_id'),
	drop_col(col='provincia_nombre', axis=1),
	drop_col(col='region_pbg', axis=1),
	rename_cols(map={'provincia_id': 'geocodigo'}),
	wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita provincial en 1895 (en pesos de 2004)'),
	replace_value(col='indicador', curr_value='var_pib_pc_1895_2022', new_value='Variación del PIB per cápita provincial entre 1895 y 2022 (1895=1)')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   provincia_id          24 non-null     object 
#   1   provincia_nombre      24 non-null     object 
#   2   region_pbg            24 non-null     object 
#   3   pib_pc_1895           24 non-null     float64
#   4   var_pib_pc_1895_2022  24 non-null     float64
#  
#  |    | provincia_id   | provincia_nombre                | region_pbg      |   pib_pc_1895 |   var_pib_pc_1895_2022 |
#  |---:|:---------------|:--------------------------------|:----------------|--------------:|-----------------------:|
#  |  0 | AR-C           | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |        6085.6 |                6.35414 |
#  
#  ------------------------------
#  
#  convert_indec_codes_to_isoprov(df_cod_col='provincia_id')
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 5 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   provincia_id          24 non-null     object 
#   1   provincia_nombre      24 non-null     object 
#   2   region_pbg            24 non-null     object 
#   3   pib_pc_1895           24 non-null     float64
#   4   var_pib_pc_1895_2022  24 non-null     float64
#  
#  |    | provincia_id   | provincia_nombre                | region_pbg      |   pib_pc_1895 |   var_pib_pc_1895_2022 |
#  |---:|:---------------|:--------------------------------|:----------------|--------------:|-----------------------:|
#  |  0 | AR-C           | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |        6085.6 |                6.35414 |
#  
#  ------------------------------
#  
#  drop_col(col='provincia_nombre', axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 4 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   provincia_id          24 non-null     object 
#   1   region_pbg            24 non-null     object 
#   2   pib_pc_1895           24 non-null     float64
#   3   var_pib_pc_1895_2022  24 non-null     float64
#  
#  |    | provincia_id   | region_pbg      |   pib_pc_1895 |   var_pib_pc_1895_2022 |
#  |---:|:---------------|:----------------|--------------:|-----------------------:|
#  |  0 | AR-C           | Pampeana y CABA |        6085.6 |                6.35414 |
#  
#  ------------------------------
#  
#  drop_col(col='region_pbg', axis=1)
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   provincia_id          24 non-null     object 
#   1   pib_pc_1895           24 non-null     float64
#   2   var_pib_pc_1895_2022  24 non-null     float64
#  
#  |    | provincia_id   |   pib_pc_1895 |   var_pib_pc_1895_2022 |
#  |---:|:---------------|--------------:|-----------------------:|
#  |  0 | AR-C           |        6085.6 |                6.35414 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo'})
#  RangeIndex: 24 entries, 0 to 23
#  Data columns (total 3 columns):
#   #   Column                Non-Null Count  Dtype  
#  ---  ------                --------------  -----  
#   0   geocodigo             24 non-null     object 
#   1   pib_pc_1895           24 non-null     float64
#   2   var_pib_pc_1895_2022  24 non-null     float64
#  
#  |    | geocodigo   |   pib_pc_1895 |   var_pib_pc_1895_2022 |
#  |---:|:------------|--------------:|-----------------------:|
#  |  0 | AR-C        |        6085.6 |                6.35414 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador   |   valor |
#  |---:|:------------|:------------|--------:|
#  |  0 | AR-C        | pib_pc_1895 |  6085.6 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='pib_pc_1895', new_value='PIB per cápita provincial en 1895 (en pesos de 2004)')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                                            |   valor |
#  |---:|:------------|:-----------------------------------------------------|--------:|
#  |  0 | AR-C        | PIB per cápita provincial en 1895 (en pesos de 2004) |  6085.6 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='var_pib_pc_1895_2022', new_value='Variación del PIB per cápita provincial entre 1895 y 2022 (1895=1)')
#  RangeIndex: 48 entries, 0 to 47
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  48 non-null     object 
#   1   indicador  48 non-null     object 
#   2   valor      48 non-null     float64
#  
#  |    | geocodigo   | indicador                                            |   valor |
#  |---:|:------------|:-----------------------------------------------------|--------:|
#  |  0 | AR-C        | PIB per cápita provincial en 1895 (en pesos de 2004) |  6085.6 |
#  
#  ------------------------------
#  