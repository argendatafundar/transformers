from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def latest_year(df, by='anio'):
    latest_year = df[by].max()
    df = df.query(f'{by} == {latest_year}')
    df = df.drop(columns = by)
    return df

@transformer.convert
def convert_indec_codes_to_isoprov2(df: DataFrame, cod_col:str, new_col:str = None):
   
   from io import StringIO
   from pandas.api.types import is_string_dtype, is_numeric_dtype
   import pandas as pd

   json_str = '[{"codprov_str":"02","codprov_iso":"AR-C","codprov_int":2},{"codprov_str":"06","codprov_iso":"AR-B","codprov_int":6},{"codprov_str":"10","codprov_iso":"AR-K","codprov_int":10},{"codprov_str":"14","codprov_iso":"AR-X","codprov_int":14},{"codprov_str":"18","codprov_iso":"AR-W","codprov_int":18},{"codprov_str":"22","codprov_iso":"AR-H","codprov_int":22},{"codprov_str":"26","codprov_iso":"AR-U","codprov_int":26},{"codprov_str":"30","codprov_iso":"AR-E","codprov_int":30},{"codprov_str":"34","codprov_iso":"AR-P","codprov_int":34},{"codprov_str":"38","codprov_iso":"AR-Y","codprov_int":38},{"codprov_str":"42","codprov_iso":"AR-L","codprov_int":42},{"codprov_str":"46","codprov_iso":"AR-F","codprov_int":46},{"codprov_str":"50","codprov_iso":"AR-M","codprov_int":50},{"codprov_str":"54","codprov_iso":"AR-N","codprov_int":54},{"codprov_str":"58","codprov_iso":"AR-Q","codprov_int":58},{"codprov_str":"62","codprov_iso":"AR-R","codprov_int":62},{"codprov_str":"66","codprov_iso":"AR-A","codprov_int":66},{"codprov_str":"70","codprov_iso":"AR-J","codprov_int":70},{"codprov_str":"74","codprov_iso":"AR-D","codprov_int":74},{"codprov_str":"78","codprov_iso":"AR-Z","codprov_int":78},{"codprov_str":"82","codprov_iso":"AR-S","codprov_int":82},{"codprov_str":"86","codprov_iso":"AR-G","codprov_int":86},{"codprov_str":"90","codprov_iso":"AR-T","codprov_int":90},{"codprov_str":"94","codprov_iso":"AR-V","codprov_int":94}]'
   diccionario : DataFrame = pd.read_json(StringIO(json_str), dtype={'codprov_str':str, 'codprov_iso':str, 'codprov_int':int})

   col_used = 'codprov_str'
   if is_numeric_dtype(df[cod_col]):
      col_used = 'codprov_int'
      
   replace_mapper = diccionario.set_index(col_used)['codprov_iso'].to_dict()
   col_name = cod_col
   
   if new_col: 
       col_name = new_col
   df[col_name] = df[cod_col].replace(replace_mapper)

   return df

@transformer.convert
def cod_depto_CABA_to_indec(df:DataFrame, col_depto:str = 'id_depto'): 
    new_df = df.copy()
    def get_last_elements(s, n = 3):
        return str(s)[-n:]
    serie = new_df[col_depto].astype(str).str.zfill(5).copy()
    bool_filter = serie.str.startswith("02")
    
    new_df.loc[bool_filter, col_depto] = 2000 + (serie[bool_filter].apply(get_last_elements).astype('int') / 7)
    return new_df

@transformer.convert
def concatenar_id_depto_con_isoprov(df:DataFrame, col_depto:str = 'id_depto', col_isoprov:str = 'iso_prov', new_col:str = 'geocodigo'): 
    def get_last_elements(s, n = 3):
        return str(s)[-n:]
    df[new_col] = df[col_isoprov] + df[col_depto].apply(get_last_elements)
    return df

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
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
latest_year(by='anio'),
	convert_indec_codes_to_isoprov2(cod_col='provincia_id', new_col='iso_prov'),
	cod_depto_CABA_to_indec(col_depto='id_depto'),
	concatenar_id_depto_con_isoprov(col_depto='id_depto', col_isoprov='iso_prov', new_col='geocodigo'),
	drop_col(col=['departamento', 'id_depto', 'provincia', 'iso_prov', 'provincia_id'], axis=1),
	rename_cols(map={'region': 'grupo', 'densidad_emp': 'Densidad empresarial', 'share_pb_nbi': '% población NBI'}),
	wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador')
)
#  PIPELINE_END


#  start()
#  RangeIndex: 529 entries, 0 to 528
#  Data columns (total 8 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   anio                        527 non-null    float64
#   1   id_depto                    529 non-null    int64  
#   2   departamento                526 non-null    object 
#   3   provincia_id                527 non-null    float64
#   4   provincia                   527 non-null    object 
#   5   densidad_emp                527 non-null    float64
#   6   porcentaje_hogares_con_nbi  527 non-null    float64
#   7   region                      527 non-null    object 
#  
#  |    |   anio |   id_depto | departamento   |   provincia_id | provincia              |   densidad_emp |   porcentaje_hogares_con_nbi | region   |
#  |---:|-------:|-----------:|:---------------|---------------:|:-----------------------|---------------:|-----------------------------:|:---------|
#  |  0 |   2022 |       2007 | Comuna 1       |              2 | Ciudad de Buenos Aires |        166.823 |                      11.5549 | Centro   |
#  
#  ------------------------------
#  
#  latest_year(by='anio')
#  Index: 527 entries, 0 to 528
#  Data columns (total 8 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   id_depto                    527 non-null    int64  
#   1   departamento                524 non-null    object 
#   2   provincia_id                527 non-null    float64
#   3   provincia                   527 non-null    object 
#   4   densidad_emp                527 non-null    float64
#   5   porcentaje_hogares_con_nbi  527 non-null    float64
#   6   region                      527 non-null    object 
#   7   iso_prov                    527 non-null    object 
#  
#  |    |   id_depto | departamento   |   provincia_id | provincia              |   densidad_emp |   porcentaje_hogares_con_nbi | region   | iso_prov   |
#  |---:|-----------:|:---------------|---------------:|:-----------------------|---------------:|-----------------------------:|:---------|:-----------|
#  |  0 |       2007 | Comuna 1       |              2 | Ciudad de Buenos Aires |        166.823 |                      11.5549 | Centro   | AR-C       |
#  
#  ------------------------------
#  
#  convert_indec_codes_to_isoprov2(cod_col='provincia_id', new_col='iso_prov')
#  Index: 527 entries, 0 to 528
#  Data columns (total 8 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   id_depto                    527 non-null    int64  
#   1   departamento                524 non-null    object 
#   2   provincia_id                527 non-null    float64
#   3   provincia                   527 non-null    object 
#   4   densidad_emp                527 non-null    float64
#   5   porcentaje_hogares_con_nbi  527 non-null    float64
#   6   region                      527 non-null    object 
#   7   iso_prov                    527 non-null    object 
#  
#  |    |   id_depto | departamento   |   provincia_id | provincia              |   densidad_emp |   porcentaje_hogares_con_nbi | region   | iso_prov   |
#  |---:|-----------:|:---------------|---------------:|:-----------------------|---------------:|-----------------------------:|:---------|:-----------|
#  |  0 |       2007 | Comuna 1       |              2 | Ciudad de Buenos Aires |        166.823 |                      11.5549 | Centro   | AR-C       |
#  
#  ------------------------------
#  
#  cod_depto_CABA_to_indec(col_depto='id_depto')
#  Index: 527 entries, 0 to 528
#  Data columns (total 9 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   id_depto                    527 non-null    int64  
#   1   departamento                524 non-null    object 
#   2   provincia_id                527 non-null    float64
#   3   provincia                   527 non-null    object 
#   4   densidad_emp                527 non-null    float64
#   5   porcentaje_hogares_con_nbi  527 non-null    float64
#   6   region                      527 non-null    object 
#   7   iso_prov                    527 non-null    object 
#   8   geocodigo                   527 non-null    object 
#  
#  |    |   id_depto | departamento   |   provincia_id | provincia              |   densidad_emp |   porcentaje_hogares_con_nbi | region   | iso_prov   | geocodigo   |
#  |---:|-----------:|:---------------|---------------:|:-----------------------|---------------:|-----------------------------:|:---------|:-----------|:------------|
#  |  0 |       2001 | Comuna 1       |              2 | Ciudad de Buenos Aires |        166.823 |                      11.5549 | Centro   | AR-C       | AR-C001     |
#  
#  ------------------------------
#  
#  concatenar_id_depto_con_isoprov(col_depto='id_depto', col_isoprov='iso_prov', new_col='geocodigo')
#  Index: 527 entries, 0 to 528
#  Data columns (total 9 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   id_depto                    527 non-null    int64  
#   1   departamento                524 non-null    object 
#   2   provincia_id                527 non-null    float64
#   3   provincia                   527 non-null    object 
#   4   densidad_emp                527 non-null    float64
#   5   porcentaje_hogares_con_nbi  527 non-null    float64
#   6   region                      527 non-null    object 
#   7   iso_prov                    527 non-null    object 
#   8   geocodigo                   527 non-null    object 
#  
#  |    |   id_depto | departamento   |   provincia_id | provincia              |   densidad_emp |   porcentaje_hogares_con_nbi | region   | iso_prov   | geocodigo   |
#  |---:|-----------:|:---------------|---------------:|:-----------------------|---------------:|-----------------------------:|:---------|:-----------|:------------|
#  |  0 |       2001 | Comuna 1       |              2 | Ciudad de Buenos Aires |        166.823 |                      11.5549 | Centro   | AR-C       | AR-C001     |
#  
#  ------------------------------
#  
#  drop_col(col=['departamento', 'id_depto', 'provincia', 'iso_prov', 'provincia_id'], axis=1)
#  Index: 527 entries, 0 to 528
#  Data columns (total 4 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   densidad_emp                527 non-null    float64
#   1   porcentaje_hogares_con_nbi  527 non-null    float64
#   2   region                      527 non-null    object 
#   3   geocodigo                   527 non-null    object 
#  
#  |    |   densidad_emp |   porcentaje_hogares_con_nbi | region   | geocodigo   |
#  |---:|---------------:|-----------------------------:|:---------|:------------|
#  |  0 |        166.823 |                      11.5549 | Centro   | AR-C001     |
#  
#  ------------------------------
#  
#  rename_cols(map={'region': 'grupo', 'densidad_emp': 'Densidad empresarial', 'share_pb_nbi': '% población NBI'})
#  Index: 527 entries, 0 to 528
#  Data columns (total 4 columns):
#   #   Column                      Non-Null Count  Dtype  
#  ---  ------                      --------------  -----  
#   0   Densidad empresarial        527 non-null    float64
#   1   porcentaje_hogares_con_nbi  527 non-null    float64
#   2   grupo                       527 non-null    object 
#   3   geocodigo                   527 non-null    object 
#  
#  |    |   Densidad empresarial |   porcentaje_hogares_con_nbi | grupo   | geocodigo   |
#  |---:|-----------------------:|-----------------------------:|:--------|:------------|
#  |  0 |                166.823 |                      11.5549 | Centro  | AR-C001     |
#  
#  ------------------------------
#  
#  wide_to_long(primary_keys=['grupo', 'geocodigo'], value_name='valor', var_name='indicador')
#  RangeIndex: 1054 entries, 0 to 1053
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   grupo      1054 non-null   object 
#   1   geocodigo  1054 non-null   object 
#   2   indicador  1054 non-null   object 
#   3   valor      1054 non-null   float64
#  
#  |    | grupo   | geocodigo   | indicador            |   valor |
#  |---:|:--------|:------------|:---------------------|--------:|
#  |  0 | Centro  | AR-C001     | Densidad empresarial | 166.823 |
#  
#  ------------------------------
#  