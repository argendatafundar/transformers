from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def to_pandas(df: DataFrame, dummy = True):
    df = df.to_pandas()
    return df

@transformer.convert
def cast_to(df: pl.DataFrame, col: str, target_type: str = "pl.Float64") -> pl.DataFrame:
    return df.with_columns([
        pl.col(col).cast(eval(target_type), strict=False)
    ])

@transformer.convert
def drop_na(df:DataFrame, col:str):
    df = df.dropna(subset=col, axis=0)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	cast_to(col='porcentaje_pbi', target_type='pl.Float64'),
	to_pandas(dummy=True),
	drop_na(col='porcentaje_pbi')
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  cast_to(col='porcentaje_pbi', target_type='pl.Float64')
#  
#  ------------------------------
#  
#  to_pandas(dummy=True)
#  RangeIndex: 63 entries, 0 to 62
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   ejercicio_presupuestario  63 non-null     int64  
#   1   tipo_gasto                63 non-null     object 
#   2   porcentaje_pbi            61 non-null     float64
#  
#  |    |   ejercicio_presupuestario | tipo_gasto   |   porcentaje_pbi |
#  |---:|---------------------------:|:-------------|-----------------:|
#  |  0 |                       2004 | APN          |          13.2495 |
#  
#  ------------------------------
#  
#  drop_na(col='porcentaje_pbi')
#  Index: 61 entries, 0 to 60
#  Data columns (total 3 columns):
#   #   Column                    Non-Null Count  Dtype  
#  ---  ------                    --------------  -----  
#   0   ejercicio_presupuestario  61 non-null     int64  
#   1   tipo_gasto                61 non-null     object 
#   2   porcentaje_pbi            61 non-null     float64
#  
#  |    |   ejercicio_presupuestario | tipo_gasto   |   porcentaje_pbi |
#  |---:|---------------------------:|:-------------|-----------------:|
#  |  0 |                       2004 | APN          |          13.2495 |
#  
#  ------------------------------
#  