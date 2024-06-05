from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df

@transformer.convert
def rename_cols(df: DataFrame, map):
    df = df.rename(columns=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
rename_cols(map={'sector': 'indicador'}),
	rename_cols(map={'prop_teletrabajo': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              21 non-null     int64  
#   1   sector            21 non-null     object 
#   2   prop_teletrabajo  21 non-null     float64
#  
#  |    |   anio | sector   |   prop_teletrabajo |
#  |---:|-------:|:---------|-------------------:|
#  |  0 |   2016 | SBC      |           0.113657 |
#  
#  ------------------------------
#  
#  rename_cols(map={'sector': 'indicador'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column            Non-Null Count  Dtype  
#  ---  ------            --------------  -----  
#   0   anio              21 non-null     int64  
#   1   indicador         21 non-null     object 
#   2   prop_teletrabajo  21 non-null     float64
#  
#  |    |   anio | indicador   |   prop_teletrabajo |
#  |---:|-------:|:------------|-------------------:|
#  |  0 |   2016 | SBC         |           0.113657 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop_teletrabajo': 'valor'})
#  RangeIndex: 21 entries, 0 to 20
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   anio       21 non-null     int64  
#   1   indicador  21 non-null     object 
#   2   valor      21 non-null     float64
#  
#  |    |   anio | indicador   |    valor |
#  |---:|-------:|:------------|---------:|
#  |  0 |   2016 | SBC         | 0.113657 |
#  
#  ------------------------------
#  