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
rename_cols(map={'provincia': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'}),
	drop_col(col='anio', axis=1)
)
#  PIPELINE_END


#  start()
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 4 columns):
#   #   Column           Non-Null Count  Dtype  
#  ---  ------           --------------  -----  
#   0   anio             864 non-null    int64  
#   1   provincia        864 non-null    object 
#   2   sector           864 non-null    object 
#   3   valor_en_mtco2e  864 non-null    float64
#  
#  |    |   anio | provincia                       | sector   |   valor_en_mtco2e |
#  |---:|-------:|:--------------------------------|:---------|------------------:|
#  |  0 |   2010 | Ciudad Autónoma de Buenos Aires | Energía  |           17.8809 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'geocodigo', 'sector': 'indicador', 'valor_en_mtco2e': 'valor'})
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 4 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       864 non-null    int64  
#   1   geocodigo  864 non-null    object 
#   2   indicador  864 non-null    object 
#   3   valor      864 non-null    float64
#  
#  |    |   anio | geocodigo                       | indicador   |   valor |
#  |---:|-------:|:--------------------------------|:------------|--------:|
#  |  0 |   2010 | Ciudad Autónoma de Buenos Aires | Energía     | 17.8809 |
#  
#  ------------------------------
#  
#  drop_col(col='anio', axis=1)
#  RangeIndex: 864 entries, 0 to 863
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   geocodigo  864 non-null    object 
#   1   indicador  864 non-null    object 
#   2   valor      864 non-null    float64
#  
#  |    | geocodigo                       | indicador   |   valor |
#  |---:|:--------------------------------|:------------|--------:|
#  |  0 | Ciudad Autónoma de Buenos Aires | Energía     | 17.8809 |
#  
#  ------------------------------
#  