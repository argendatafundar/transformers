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
def agregar_filas(df:DataFrame, nuevas_filas:dict[str,list]): 
    from pandas import concat
    nuevas_filas = DataFrame(nuevas_filas)
    df = concat([df, nuevas_filas], axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
convert_indec_codes_to_isoprov(df_cod_col='provincia_id'),
	drop_col(col='provincia_nombre', axis=1),
	drop_col(col='region_pbg', axis=1),
	rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc_relativo': 'valor'}),
	agregar_filas(nuevas_filas={
        'geocodigo': ['AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC'], 
        'anio': [1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022], 
        'valor': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]
        })
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      672 non-null    object 
#   1   provincia_nombre  672 non-null    object 
#   2   region_pbg        672 non-null    object 
#   3   anio              672 non-null    int64  
#   4   pib_pc_relativo   672 non-null    float64
#  
#  |    | provincia_id   | provincia_nombre                | region_pbg      |   anio |   pib_pc_relativo |
#  |---:|:---------------|:--------------------------------|:----------------|-------:|------------------:|
#  |  0 | AR-C           | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   1895 |            139.29 |
#  
#  ------------------------------
#  
#  convert_indec_codes_to_isoprov(df_cod_col='provincia_id')
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 5 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   provincia_id      672 non-null    object 
#   1   provincia_nombre  672 non-null    object 
#   2   region_pbg        672 non-null    object 
#   3   anio              672 non-null    int64  
#   4   pib_pc_relativo   672 non-null    float64
#  
#  |    | provincia_id   | provincia_nombre                | region_pbg      |   anio |   pib_pc_relativo |
#  |---:|:---------------|:--------------------------------|:----------------|-------:|------------------:|
#  |  0 | AR-C           | Ciudad Autónoma de Buenos Aires | Pampeana y CABA |   1895 |            139.29 |
#  
#  ------------------------------
#  
#  drop_col(col='provincia_nombre', axis=1)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   provincia_id     672 non-null    object 
#   1   region_pbg       672 non-null    object 
#   2   anio             672 non-null    int64  
#   3   pib_pc_relativo  672 non-null    float64
#  
#  |    | provincia_id   | region_pbg      |   anio |   pib_pc_relativo |
#  |---:|:---------------|:----------------|-------:|------------------:|
#  |  0 | AR-C           | Pampeana y CABA |   1895 |            139.29 |
#  
#  ------------------------------
#  
#  drop_col(col='region_pbg', axis=1)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 3 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   provincia_id     672 non-null    object 
#   1   anio             672 non-null    int64  
#   2   pib_pc_relativo  672 non-null    float64
#  
#  |    | provincia_id   |   anio |   pib_pc_relativo |
#  |---:|:---------------|-------:|------------------:|
#  |  0 | AR-C           |   1895 |            139.29 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc_relativo': 'valor'})
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  672 non-null    object 
#   1   anio       672 non-null    int64  
#   2   valor      672 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   1895 |  139.29 |
#  
#  ------------------------------
#  
#  agregar_filas(nuevas_filas={'geocodigo': ['AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC', 'AR-NAC'], 'anio': array([1895, 1914, 1937, 1946, 1953, 1965, 1975, 1986, 1993, 2004, 2005,
#         2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016,
#         2017, 2018, 2019, 2020, 2021, 2022]), 'valor': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]})
#  Index: 700 entries, 0 to 27
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  700 non-null    object 
#   1   anio       700 non-null    int64  
#   2   valor      700 non-null    float64
#  
#  |    | geocodigo   |   anio |   valor |
#  |---:|:------------|-------:|--------:|
#  |  0 | AR-C        |   1895 |  139.29 |
#  
#  ------------------------------
#  