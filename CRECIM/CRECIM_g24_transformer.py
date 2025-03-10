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
rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc': 'valor'}),
	drop_col(col=['region_pbg'], axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 4 columns):
#   #   Column        Non-Null Count  Dtype  
#  ---  ------        --------------  -----  
#   0   region_pbg    672 non-null    object 
#   1   anio          672 non-null    int64  
#   2   pib_pc        672 non-null    float64
#   3   provincia_id  672 non-null    object 
#  
#  |    | region_pbg      |   anio |   pib_pc | provincia_id   |
#  |---:|:----------------|-------:|---------:|:---------------|
#  |  0 | Pampeana y AMBA |   1895 |  4413.34 | AR-B           |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia_id': 'geocodigo', 'pib_pc': 'valor'})
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
#  |  0 | Pampeana y AMBA |   1895 | 4413.34 | AR-B        |
#  
#  ------------------------------
#  
#  drop_col(col=['region_pbg'], axis=1)
#  RangeIndex: 672 entries, 0 to 671
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       672 non-null    int64  
#   1   valor      672 non-null    float64
#   2   geocodigo  672 non-null    object 
#  
#  |    |   anio |   valor | geocodigo   |
#  |---:|-------:|--------:|:------------|
#  |  0 |   1895 | 4413.34 | AR-B        |
#  
#  ------------------------------
#  