from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def query(df: DataFrame, condition: str):
    df = df.query(condition)    
    return df

@transformer.convert
def drop_col(df: DataFrame, col, axis=1):
    return df.drop(col, axis=axis)
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc_relativo': 'valor'}),
	query(condition='anio == anio.max()'),
	drop_col(col=['anio', 'region_pbg'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   region_pbg       672 non-null    object 
#   1   anio             672 non-null    int64  
#   2   pib_pc_relativo  672 non-null    float64
#   3   provincia_id     672 non-null    object 
#  
#  |    | region_pbg      |   anio |   pib_pc_relativo | provincia_id   |
#  |---:|:----------------|-------:|------------------:|:---------------|
#  |  0 | Pampeana y AMBA |   1895 |           102.682 | AR-B           |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc_relativo': 'valor'})
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   region_pbg  672 non-null    object 
#   1   anio        672 non-null    int64  
#   2   valor       672 non-null    float64
#   3   geocodigo   672 non-null    object 
#  
#  |    | region_pbg      |   anio |   valor | geocodigo   |
#  |---:|:----------------|-------:|--------:|:------------|
#  |  0 | Pampeana y AMBA |   1895 | 102.682 | AR-B        |
#  
#  ------------------------------
#  
#  query(condition='anio == anio.max()')
#  Index: 24 entries, 27 to 671
#  Data columns (total 4 columns):
#   #   Column      Non-Null Count  Dtype  
#  ---  ------      --------------  -----  
#   0   region_pbg  24 non-null     object 
#   1   anio        24 non-null     int64  
#   2   valor       24 non-null     float64
#   3   geocodigo   24 non-null     object 
#  
#  |    | region_pbg      |   anio |   valor | geocodigo   |
#  |---:|:----------------|-------:|--------:|:------------|
#  | 27 | Pampeana y AMBA |   2022 | 84.1437 | AR-B        |
#  
#  ------------------------------
#  
#  drop_col(col=['anio', 'region_pbg'], axis=1)
#  Index: 24 entries, 27 to 671
#  Data columns (total 2 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   valor      24 non-null     float64
#   1   geocodigo  24 non-null     object 
#  
#  |    |   valor | geocodigo   |
#  |---:|--------:|:------------|
#  | 27 | 84.1437 | AR-B        |
#  
#  ------------------------------
#  