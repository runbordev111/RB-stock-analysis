import shioaji as sj
print(sj.__version__)
# 1.0.0

api = sj.Shioaji(simulation=True) # 模擬模式
api.login(
    api_key="14WkzEdjiPEPzaS6LF9Hwa1aFDKBNsPN68A3HLxBF7d",     # 請修改此處
    secret_key="79VwzxE3rrXpD6UzqydMvtCThfUvN4cNbZqeonoxVNXu"   # 請修改此處
)

# 商品檔 - 請修改此處
contract = api.Contracts.Stocks.TSE["2890"]

# 證券委託單 - 請修改此處
order = api.Order(
    price=29,                                       # 價格
    quantity=1,                                     # 數量
    action=sj.constant.Action.Buy,                  # 買賣別
    price_type=sj.constant.StockPriceType.LMT,      # 委託價格類別
    order_type=sj.constant.OrderType.ROD,           # 委託條件
    account=api.stock_account                       # 下單帳號
)

# 下單
trade = api.place_order(contract, order)
trade