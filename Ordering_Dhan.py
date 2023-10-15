from dhanhq import dhanhq

access_token = ("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNjk5NTQ3MzI"
                "yLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDY3ODUzNCJ9."
                "A079FtuGGqgPUhf3muUP9VFKfX1YwGM2b63wQZpvJyvw2f0o1jWZTQKzSBY6LiaM94L-nNEI1OSbdZFHgJGOcA")
client_id = "1100678534"
dhan = dhanhq(client_id, access_token)
f = dhan.get_fund_limits()
print(f)

h = dhan.get_holdings()
p = dhan.get_positions()

print(p['data'][1]['securityId'])
opt1 = p['data'][1]['securityId']
print(p['data'][1])



dhan.place_order(
    tag='',
    transaction_type=dhan.SELL,
    exchange_segment=dhan.FNO,
    product_type=dhan.MARGIN,
    order_type=dhan.MARKET,
    validity='DAY',
    security_id='134673',
    quantity=800,
    disclosed_quantity=0,
    price=0,
    trigger_price=0,
    after_market_order=False,
    amo_time='OPEN',
    bo_profit_value=0,
    bo_stop_loss_Value=0,
    drv_expiry_date='2023-10-26',
    drv_options_type='CALL',
    drv_strike_price=1200.0
)

