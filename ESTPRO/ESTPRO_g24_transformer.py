from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

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
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)

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
query(condition='anio != 2020'),
	query(condition='anio == anio.max()'),
	convert_indec_codes_to_isoprov(df_cod_col='provincia_id'),
	rename_cols(map={'region': 'grupo', 'provincia_id': 'geocodigo'}),
	drop_col(col=['anio', 'id_depto', 'departamento', 'provincia'], axis=1),
	wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador'),
	replace_value(col='indicador', curr_value='densidad_emp', new_value='Densidad empresarial'),
	replace_value(col='indicador', curr_value='share_pb_nbi', new_value='% población NBI')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 525 entries, 0 to 524
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          525 non-null    int64  
#   1   id_depto      525 non-null    int64  
#   2   departamento  525 non-null    object 
#   3   provincia_id  525 non-null    int64  
#   4   provincia     525 non-null    object 
#   5   region        525 non-null    object 
#   6   densidad_emp  525 non-null    float64
#   7   share_pb_nbi  525 non-null    float64
#  
#  |    |   anio |   id_depto | departamento   |   provincia_id | provincia   | region   |   densidad_emp |   share_pb_nbi |
#  |---:|-------:|-----------:|:---------------|---------------:|:------------|:---------|---------------:|---------------:|
#  |  0 |   2022 |       2007 | Comuna 1       |              2 | CABA        | Centro   |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  query(condition='anio != 2020')
#  Index: 525 entries, 0 to 524
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          525 non-null    int64  
#   1   id_depto      525 non-null    int64  
#   2   departamento  525 non-null    object 
#   3   provincia_id  525 non-null    int64  
#   4   provincia     525 non-null    object 
#   5   region        525 non-null    object 
#   6   densidad_emp  525 non-null    float64
#   7   share_pb_nbi  525 non-null    float64
#  
#  |    |   anio |   id_depto | departamento   |   provincia_id | provincia   | region   |   densidad_emp |   share_pb_nbi |
#  |---:|-------:|-----------:|:---------------|---------------:|:------------|:---------|---------------:|---------------:|
#  |  0 |   2022 |       2007 | Comuna 1       |              2 | CABA        | Centro   |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 525 entries, 0 to 524
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          525 non-null    int64  
#   1   id_depto      525 non-null    int64  
#   2   departamento  525 non-null    object 
#   3   provincia_id  525 non-null    object 
#   4   provincia     525 non-null    object 
#   5   region        525 non-null    object 
#   6   densidad_emp  525 non-null    float64
#   7   share_pb_nbi  525 non-null    float64
#  
#  |    |   anio |   id_depto | departamento   | provincia_id   | provincia   | region   |   densidad_emp |   share_pb_nbi |
#  |---:|-------:|-----------:|:---------------|:---------------|:------------|:---------|---------------:|---------------:|
#  |  0 |   2022 |       2007 | Comuna 1       | AR-C           | CABA        | Centro   |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  convert_indec_codes_to_isoprov(df_cod_col='provincia_id')
#  Index: 525 entries, 0 to 524
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          525 non-null    int64  
#   1   id_depto      525 non-null    int64  
#   2   departamento  525 non-null    object 
#   3   provincia_id  525 non-null    object 
#   4   provincia     525 non-null    object 
#   5   region        525 non-null    object 
#   6   densidad_emp  525 non-null    float64
#   7   share_pb_nbi  525 non-null    float64
#  
#  |    |   anio |   id_depto | departamento   | provincia_id   | provincia   | region   |   densidad_emp |   share_pb_nbi |
#  |---:|-------:|-----------:|:---------------|:---------------|:------------|:---------|---------------:|---------------:|
#  |  0 |   2022 |       2007 | Comuna 1       | AR-C           | CABA        | Centro   |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'grupo', 'provincia_id': 'geocodigo'})
#  Index: 525 entries, 0 to 524
#  Data columns (total 8 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   anio          525 non-null    int64  
#   1   id_depto      525 non-null    int64  
#   2   departamento  525 non-null    object 
#   3   geocodigo     525 non-null    object 
#   4   provincia     525 non-null    object 
#   5   grupo         525 non-null    object 
#   6   densidad_emp  525 non-null    float64
#   7   share_pb_nbi  525 non-null    float64
#  
#  |    |   anio |   id_depto | departamento   | geocodigo   | provincia   | grupo   |   densidad_emp |   share_pb_nbi |
#  |---:|-------:|-----------:|:---------------|:------------|:------------|:--------|---------------:|---------------:|
#  |  0 |   2022 |       2007 | Comuna 1       | AR-C        | CABA        | Centro  |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'id_depto', 'departamento', 'provincia'], axis=1)
#  Index: 525 entries, 0 to 524
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   geocodigo     525 non-null    object 
#   1   grupo         525 non-null    object 
#   2   densidad_emp  525 non-null    float64
#   3   share_pb_nbi  525 non-null    float64
#  
#  |    | geocodigo   | grupo   |   densidad_emp |   share_pb_nbi |
#  |---:|:------------|:--------|---------------:|---------------:|
#  |  0 | AR-C        | Centro  |        209.837 |        18.1215 |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 1050 entries, 0 to 1049
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      1050 non-null   object 
#   1   geocodigo  1050 non-null   object 
#   2   indicador  1050 non-null   object 
#   3   valor      1050 non-null   float64
#  
#  |    | grupo   | geocodigo   | indicador    |   valor |
#  |---:|:--------|:------------|:-------------|--------:|
#  |  0 | Centro  | AR-C        | densidad_emp | 209.837 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='densidad_emp', new_value='Densidad empresarial')
#  RangeIndex: 1050 entries, 0 to 1049
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      1050 non-null   object 
#   1   geocodigo  1050 non-null   object 
#   2   indicador  1050 non-null   object 
#   3   valor      1050 non-null   float64
#  
#  |    | grupo   | geocodigo   | indicador            |   valor |
#  |---:|:--------|:------------|:---------------------|--------:|
#  |  0 | Centro  | AR-C        | Densidad empresarial | 209.837 |
#  
#  ------------------------------
#  
#  replace_value(col='indicador', curr_value='share_pb_nbi', new_value='% población NBI')
#  RangeIndex: 1050 entries, 0 to 1049
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      1050 non-null   object 
#   1   geocodigo  1050 non-null   object 
#   2   indicador  1050 non-null   object 
#   3   valor      1050 non-null   float64
#  
#  |    | grupo   | geocodigo   | indicador            |   valor |
#  |---:|:--------|:------------|:---------------------|--------:|
#  |  0 | Centro  | AR-C        | Densidad empresarial | 209.837 |
#  
#  ------------------------------
#  