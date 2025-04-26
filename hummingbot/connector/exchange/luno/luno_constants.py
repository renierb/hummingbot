from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "h-luno"
MAX_ORDER_ID_LEN = 32

# Base URLs
REST_URL = "https://api.luno.com/api/"
WSS_URL = "wss://ws.luno.com/api/1/stream/{}"

# API Endpoints
# Public API
TICKER_URL = "1/ticker"
TICKERS_URL = "1/tickers"
ORDERBOOK_URL = "1/orderbook"
ORDERBOOK_TOP_URL = "1/orderbook_top"
TRADES_URL = "1/trades"
MARKETS_INFO_URL = "exchange/1/markets"
CANDLES_URL = "exchange/1/candles"

# Private API
ACCOUNTS_URL = "1/balance"
PENDING_TRANSACTIONS_URL = "1/accounts/{}/pending"
TRANSACTIONS_URL = "1/accounts/{}/transactions"
SEND_URL = "1/send"
TRANSFERS_URL = "exchange/1/transfers"
BENEFICIARIES_URL = "1/beneficiaries"
# Orders
LIMIT_ORDER_URL = "1/postorder"
MARKET_ORDER_URL = "1/marketorder"
LIST_ORDERS_URL = "exchange/2/listorders"  # v2 endpoint
STOP_ORDER_URL = "1/stoporder"
GET_ORDER_URL = "exchange/3/order"  # v3 endpoint
FEE_INFO_URL = "1/fee_info"

# WebSocket channels
WS_HEARTBEAT_TIME_INTERVAL = 30

# Order type definitions
SIDE_BUY = "BUY"
SIDE_SELL = "SELL"

# Limit order side values
LIMIT_SIDE_BID = "BID"  # Buy
LIMIT_SIDE_ASK = "ASK"  # Sell

TIME_IN_FORCE_GTC = "GTC"  # Good till cancelled
TIME_IN_FORCE_IOC = "IOC"  # Immediate or cancel
TIME_IN_FORCE_FOK = "FOK"  # Fill or kill

# Order type mapping
ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_MARKET = "MARKET"
ORDER_TYPE_LIMIT_MAKER = "LIMIT_MAKER"

# Rate Limit Type
REQUEST_WEIGHT = "REQUEST_WEIGHT"
ORDERS = "ORDERS"
ORDERS_24HR = "ORDERS_24HR"
RAW_REQUESTS = "RAW_REQUESTS"

# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

MAX_REQUEST = 300  # Default max requests per minute

# Luno Order States mapping to Hummingbot OrderState
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "AWAITING": OrderState.OPEN,
    "COMPLETE": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED
}

# WebSocket event types
DIFF_EVENT_TYPE = "orderBookUpdate"  # Used for both create_update and delete_update
TRADE_EVENT_TYPE = "tradeUpdate"     # Used for trade_updates

# Rate limits based on Luno API docs
RATE_LIMITS = [
    # Pools
    RateLimit(limit_id=REQUEST_WEIGHT, limit=300, time_interval=ONE_MINUTE),
    RateLimit(limit_id=ORDERS, limit=10, time_interval=10 * ONE_SECOND),
    RateLimit(limit_id=RAW_REQUESTS, limit=300, time_interval=ONE_MINUTE),

    # Weighted Limits for specific endpoints
    RateLimit(limit_id=TICKER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TICKERS_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDERBOOK_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ORDERBOOK_TOP_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=TRADES_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=ACCOUNTS_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=LIMIT_ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(ORDERS, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=MARKET_ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(ORDERS, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=STOP_ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=GET_ORDER_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=LIST_ORDERS_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 5),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=FEE_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=SEND_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 2),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
    RateLimit(limit_id=MARKETS_INFO_URL, limit=MAX_REQUEST, time_interval=ONE_MINUTE,
              linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT, 1),
                             LinkedLimitWeightPair(RAW_REQUESTS, 1)]),
]

# Error codes and messages
ORDER_NOT_EXIST_ERROR_CODE = "ErrOrderNotFound"
ORDER_NOT_EXIST_MESSAGE = "Cannot find that order"
UNKNOWN_ORDER_ERROR_CODE = "ErrInvalidOrderRef"
UNKNOWN_ORDER_MESSAGE = "Order reference is invalid"

# WebSocket reconnection parameters
MAX_RECONNECT_ATTEMPTS = 10
BASE_RECONNECT_DELAY = 1.0
