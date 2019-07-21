import pandas as pd
# from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

# te = TransactionEncoder()
df = pd.read_csv('anime_list.csv',header=None)
# df = te.fit(df).transform(df, sparse=True)
# sparse_df = pd.SparseDataFrame(df, columns=te.columns_, default_fill_value=False)

def get_rules(df,support,confidence,n):
    # 获取support>=指定阈值的频繁项集
    frequent_itemsets = apriori(df, min_support=support, use_colnames=True,low_memory=True)
    # 获取confidence>=指定阈值的的关联规则
    rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=confidence)
    # 将获取的rule按照confidence降序排序
    rules.sort_values(by='confidence', ascending=False)
    # 获取confidence前10的rule
    if len(rules)>10:
      return rules[0:n]
    else:
      return rules

result = get_rules(df=df, support=0.03, confidence=0.7, n=10)

print result