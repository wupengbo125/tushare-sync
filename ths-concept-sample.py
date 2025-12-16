import adata
df = adata.stock.market.get_market_concept_ths(index_code="885551")
df.to_csv("ths_concept_sample.csv", index=False, encoding="utf-8-sig")
