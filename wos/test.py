import re
query = "IEEE TRANSACTIONS ON VISUALIZATION AND COMPUTER GRAPHICS normal OR NOT NORMAL"
query = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), query)
print(query)