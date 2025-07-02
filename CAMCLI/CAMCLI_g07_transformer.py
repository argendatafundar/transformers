from pandas import DataFrame
from data_transformers import chain, transformer


#  DEFINITIONS_START
@transformer.convert
def recalculate_shares(df: pl.DataFrame, group_col: str, value_col:str):
    # Group by the specified column and calculate shares within each group
    df = df.with_columns([
        pl.col(value_col).sum().over(group_col).alias('group_total')
    ]).with_columns([
        (pl.col(value_col) * 100 / pl.col('group_total')).alias('y')
    ]).drop('group_total')
    return df

@transformer.convert
def round(df: pl.DataFrame, col, digits):
    df = df.with_columns([
        pl.col(col).round(digits).alias(col)
    ])
    return df

@transformer.convert
def rename_cols(df: pl.DataFrame, map):
    df = df.rename(mapping=map)
    return df
#  DEFINITIONS_END


#  PIPELINE_START
pipeline = chain(
	rename_cols(map={'geonombreFundar': 'x', 'sector': 'categoria', 'valor_en_porcent': 'y'}),
	round(col='y', digits=1),
	recalculate_shares(group_col='x', value_col='y'),
	round(col='y', digits=1)
)
#  PIPELINE_END


#  start()
#  
#  ------------------------------
#  
#  rename_cols(map={'geonombreFundar': 'x', 'sector': 'categoria', 'valor_en_porcent': 'y'})
#  
#  ------------------------------
#  
#  round(col='y', digits=1)
#  
#  ------------------------------
#  
#  recalculate_shares(group_col='x', value_col='y')
#  
#  ------------------------------
#  
#  round(col='y', digits=1)
#  
#  ------------------------------
#  