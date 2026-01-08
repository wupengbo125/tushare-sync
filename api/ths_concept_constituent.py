import adata
# 获取同花顺概念成分股
df = adata.stock.info.concept_constituent_ths(index_code="885556")
print(df)
