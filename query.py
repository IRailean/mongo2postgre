orders_create = """CREATE TABLE orders 
                    (id                 BIGINT PRIMARY KEY, 
                    created_at          TIMESTAMP, 
                    date_tz             TIMESTAMP, 
                    item_count          BIGINT, 
                    order_id            TEXT, 
                    receive_method      TEXT, 
                    status              TEXT, 
                    store_id            TEXT, 
                    subtotal            DOUBLE PRECISION, 
                    tax_percentage      DOUBLE PRECISION,
                    total               DOUBLE PRECISION,
                    total_discount      DOUBLE PRECISION,
                    total_gratuity      DOUBLE PRECISION,
                    total_tax           DOUBLE PRECISION, 
                    updated_at          TIMESTAMP, 
                    user_id             BIGINT, 
                    fulfillment_date_tz TIMESTAMP);"""
orders_upsert = """
                INSERT INTO orders (id, created_at, date_tz, item_count, order_id, receive_method, status, store_id, subtotal, 
                 tax_percentage, total, total_discount, total_gratuity, total_tax, 
                     updated_at, user_id, fulfillment_date_tz) 
                VALUES %s 
                ON CONFLICT (id) DO UPDATE 
                SET updated_at          = EXCLUDED.updated_at,
                    date_tz             = EXCLUDED.date_tz,
                    item_count          = EXCLUDED.item_count,
                    receive_method      = EXCLUDED.receive_method,
                    status              = EXCLUDED.status,
                    subtotal            = EXCLUDED.subtotal,
                    tax_percentage      = EXCLUDED.tax_percentage,
                    total               = EXCLUDED.total,
                    total_discount      = EXCLUDED.total_discount,
                    total_gratuity      = EXCLUDED.total_gratuity,
                    total_tax           = EXCLUDED.total_tax,
                    user_id             = EXCLUDED.user_id,
                    fulfillment_date_tz = EXCLUDED.fulfillment_date_tz
                    """
users_create =  """CREATE TABLE users 
                    (user_id     BIGINT PRIMARY KEY, 
                    first_name   TEXT, 
                    last_name    TEXT, 
                    merchant_id  TEXT, 
                    phone_number BIGINT, 
                    created_at   TIMESTAMP,
                    updated_at   TIMESTAMP);"""

users_upsert = """
                INSERT INTO users (user_id, first_name, last_name, merchant_id, phone_number, created_at, updated_at) 
                VALUES %s 
                ON CONFLICT (user_id) DO UPDATE 
                SET phone_number = EXCLUDED.phone_number,
                    merchant_id  = EXCLUDED.merchant_id,
                    updated_at   = EXCLUDED.updated_at,
                    first_name   = EXCLUDED.first_name,
                    last_name    = EXCLUDED.last_name
                    """