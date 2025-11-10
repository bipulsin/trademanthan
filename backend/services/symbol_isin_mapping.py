"""
Symbol to ISIN mapping for NSE stocks
ISIN (International Securities Identification Number) format: INE######A##
"""

# Top 100 NSE stocks with their ISIN codes
SYMBOL_TO_ISIN = {
    # Nifty 50 Stocks
    "RELIANCE": "INE002A01018",
    "TCS": "INE467B01029",
    "HDFCBANK": "INE040A01034",
    "INFY": "INE009A01021",
    "ICICIBANK": "INE090A01021",
    "HINDUNILVR": "INE030A01027",
    "ITC": "INE154A01025",
    "SBIN": "INE062A01020",
    "BHARTIARTL": "INE397D01024",
    "KOTAKBANK": "INE237A01028",
    "LT": "INE018A01030",
    "AXISBANK": "INE238A01034",
    "BAJFINANCE": "INE296A01024",
    "ASIANPAINT": "INE021A01026",
    "MARUTI": "INE585B01010",
    "HCLTECH": "INE860A01027",
    "TITAN": "INE280A01028",
    "SUNPHARMA": "INE044A01036",
    "ULTRACEMCO": "INE481G01011",
    "NESTLEIND": "INE239A01024",
    "WIPRO": "INE075A01022",
    "POWERGRID": "INE752E01010",
    "NTPC": "INE733E01010",
    "M&M": "INE101A01026",
    "TATAMOTORS": "INE155A01022",
    "TATASTEEL": "INE081A01020",
    "TECHM": "INE669C01036",
    "ONGC": "INE213A01029",
    "BAJAJFINSV": "INE918I01018",
    "ADANIENT": "INE423A01024",
    "ADANIPORTS": "INE742F01042",
    "COALINDIA": "INE522F01014",
    "JSWSTEEL": "INE019A01038",
    "GRASIM": "INE047A01021",
    "HINDALCO": "INE038A01020",
    "DIVISLAB": "INE361B01024",
    "BAJAJ-AUTO": "INE917I01010",
    "HEROMOTOCO": "INE158A01026",
    "INDUSINDBK": "INE095A01012",
    "DRREDDY": "INE089A01023",
    "BRITANNIA": "INE216A01030",
    "EICHERMOT": "INE066A01021",
    "CIPLA": "INE059A01026",
    "UPL": "INE628A01036",
    "APOLLOHOSP": "INE437A01024",
    "BPCL": "INE029A01011",
    "TATACONSUM": "INE192A01025",
    "SBILIFE": "INE123W01016",
    "HDFCLIFE": "INE795G01014",
    
    # Additional Popular Stocks
    "VEDL": "INE205A01025",
    "TATACHEM": "INE092A01019",
    "ADANIGREEN": "INE364U01010",
    "ADANIPOWER": "INE814H01011",
    "BANDHANBNK": "INE545U01014",
    "BANKBARODA": "INE028A01039",
    "PNB": "INE160A01022",
    "CANBK": "INE476A01022",
    "IOC": "INE242A01010",
    "SAIL": "INE114A01011",
    "NMDC": "INE584A01023",
    "DLF": "INE271C01023",
    "GODREJCP": "INE102D01028",
    "GODREJPROP": "INE484J01027",
    "OBEROIRLTY": "INE093I01010",
    "HAVELLS": "INE176B01034",
    "AMBUJACEM": "INE079A01024",
    "ACC": "INE012A01025",
    "SHREECEM": "INE070A01015",
    "PEL": "INE140A01024",
    "VOLTAS": "INE226A01021",
    "BOSCHLTD": "INE323A01026",
    "MOTHERSON": "INE775A01035",
    "MUTHOOTFIN": "INE414G01012",
    "CHOLAFIN": "INE121A01024",
    "LICHSGFIN": "INE013A01015",
    "RECLTD": "INE020B01018",
    "PFC": "INE134E01011",
    "IRFC": "INE053F01010",
    "PETRONET": "INE347G01014",
    "GAIL": "INE129A01019",
    "PIDILITIND": "INE318A01026",
    "BERGEPAINT": "INE463A01038",
    "MRF": "INE883A01011",
    "APOLLOTYRE": "INE438A01022",
    "CEAT": "INE482A01020",
    "TVSMOTOR": "INE494B01023",
    "BAJAJHLDNG": "INE118A01012",
    "SIEMENS": "INE003A01024",
    "ABB": "INE117A01022",
    "CROMPTON": "INE299U01018",
    "CUMMINSIND": "INE298A01020",
    "ESCORTS": "INE042A01014",
    "ASHOKLEY": "INE208A01029",
    "TATAPOWER": "INE245A01021",
    "TORNTPOWER": "INE813H01021",
    "TORNTPHARM": "INE685A01028",
    "LUPIN": "INE326A01037",
    "BIOCON": "INE376G01013",
    "AUROPHARMA": "INE406A01037",
    "ZYDUSLIFE": "INE010B01027",
    "DMART": "INE192R01011",
    "TRENT": "INE849A01020",
    
    # Midcap Popular Stocks
    "PAGEIND": "INE761H01022",
    "MARICO": "INE196A01026",
    "DABUR": "INE016A01026",
    "COLPAL": "INE259A01022",
    "PGHH": "INE179A01014",
    "NAUKRI": "INE663F01024",
    "ZOMATO": "INE758T01015",
    "PAYTM": "INE982J01020",
    "POLICYBZR": "INE417T01012",
    "ZEEL": "INE256A01028",
    "SUNTV": "INE424H01027",
    "PVR": "INE191H01014",
    "JUBLFOOD": "INE797F01012",
    "IDFCFIRSTB": "INE092T01019",
    "FEDERALBNK": "INE171A01029",
    "RBLBANK": "INE976G01028",
    "YESBANK": "INE528G01027",
    "M&MFIN": "INE774D01024",
    "SHRIRAMFIN": "INE721A01013",
    "SRTRANSFIN": "INE804I01017",
    "ABFRL": "INE647O01011",
    "DIXON": "INE935N01012",
    "AMBER": "INE156P01015",
    "POLYCAB": "INE455K01017",
    "KEI": "INE878B01027",
    "ASTRAL": "INE006I01046",
    "BALKRISIND": "INE787D01026",
    "AARTI": "INE769A01020",
    "DEEPAKNI": "INE288B01029",
    "SRF": "INE647A01010",
    "PIDILITE": "INE318A01026",
    
    # Small/Midcap High Volume
    "SEPOWER": "INE964H01014",
    "ASTEC": "INE563J01010",
    "EDUCOMP": "INE216H01027",
    "KSERASERA": "INE506F01010",
    "IOLCP": "INE485A01015",
    "GUJAPOLLO": "INE826B01019",
    "EMCO": "INE155H01016",
    
    # Additional stocks from Nov 10, 2025 alerts (extracted from instruments JSON)
    "BHEL": "INE257A01026",  # BHARAT HEAVY ELECTRICALS
    "ETERNAL": "INE758T01015",  # ETERNAL LIMITED
    "MAXHEALTH": "INE027H01010",  # MAX HEALTHCARE INS LTD
    "PIIND": "INE603J01030",  # PI INDUSTRIES LTD
    "POWERINDIA": "INE07Y701011",  # HITACHI ENERGY INDIA LTD
    "AUBANK": "INE949L01017",  # AU SMALL FINANCE BANK LTD
    "ICICIPRULI": "INE726G01019",  # ICICI PRU LIFE INS CO LTD
    "UNIONBANK": "INE692A01016",  # UNION BANK OF INDIA
    "NUVAMA": "INE531F01015",  # NUVAMA WEALTH MANAGE LTD
    "MFSL": "INE180A01020",  # MAX FINANCIAL SERV LTD
    "LAURUSLABS": "INE947Q01028",  # LAURUS LABS LIMITED
    "MUTHOOTFIN": "INE414G01012",  # MUTHOOT FINANCE LIMITED
    "UNOMINDA": "INE405E01023",  # UNO MINDA LIMITED
    "SHREECEM": "INE070A01015",  # SHREE CEMENT LIMITED
    # Note: BAJFINANCE, ULTRACEMCO, LUPIN already exist above
}

def get_isin(symbol: str) -> str:
    """
    Get ISIN code for a given stock symbol
    
    Args:
        symbol: Stock symbol (e.g., "RELIANCE")
        
    Returns:
        ISIN code if found, otherwise symbol itself
    """
    symbol = symbol.strip().upper()
    
    # Remove common suffixes
    symbol = symbol.replace("-EQ", "").replace(".NS", "").replace(".BO", "")
    
    return SYMBOL_TO_ISIN.get(symbol, None)

def get_instrument_key(symbol: str, exchange: str = "NSE_EQ") -> str:
    """
    Get Upstox instrument key for a symbol
    
    Args:
        symbol: Stock symbol (e.g., "RELIANCE")
        exchange: Exchange identifier (default: NSE_EQ)
        
    Returns:
        Instrument key in format: NSE_EQ|INE######A##
    """
    isin = get_isin(symbol)
    
    if isin:
        return f"{exchange}|{isin}"
    else:
        # Fallback to simple format (may not work)
        return f"{exchange}|{symbol.strip().upper()}"

def is_symbol_supported(symbol: str) -> bool:
    """
    Check if symbol is supported (has ISIN mapping)
    
    Args:
        symbol: Stock symbol
        
    Returns:
        True if symbol has ISIN mapping, False otherwise
    """
    symbol = symbol.strip().upper()
    return symbol in SYMBOL_TO_ISIN

def get_all_symbols():
    """Get list of all supported symbols"""
    return list(SYMBOL_TO_ISIN.keys())

# Summary
print(f"Loaded {len(SYMBOL_TO_ISIN)} stock symbols with ISIN mappings")

