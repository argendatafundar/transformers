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
rename_cols(map={'provincia': 'indicador'}),
	rename_cols(map={'prop': 'valor'})
)
#  PIPELINE_END


#  start()
#  RangeIndex: 675 entries, 0 to 674
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   provincia  675 non-null    object 
#   1   anio       675 non-null    int64  
#   2   prop       675 non-null    float64
#  
#  |    | provincia   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  rename_cols(map={'provincia': 'indicador'})
#  RangeIndex: 675 entries, 0 to 674
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  675 non-null    object 
#   1   anio       675 non-null    int64  
#   2   prop       675 non-null    float64
#  
#  |    | indicador   |   anio |     prop |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  
#  rename_cols(map={'prop': 'valor'})
#  RangeIndex: 675 entries, 0 to 674
#  Data columns (total 3 columns):
#   #   Column     Non-Null Count  Dtype  
#  ---  ------     --------------  -----  
#   0   indicador  675 non-null    object 
#   1   anio       675 non-null    int64  
#   2   valor      675 non-null    float64
#  
#  |    | indicador   |   anio |    valor |
#  |---:|:------------|-------:|---------:|
#  |  0 | CABA        |   1996 | 0.410583 |
#  
#  ------------------------------
#  