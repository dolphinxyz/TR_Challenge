WITH
    latest_price AS (
        SELECT DISTINCT ON(instrument_id)
            instrument_id,
            date,
            price AS latest_price
        FROM prices
        ORDER BY instrument_id, date DESC),
    latest_currencies AS (
        SELECT DISTINCT ON(currency)
            currency,
            date,
            price AS latest_price
        FROM currencies
        ORDER BY currency, date DESC),
	accounts_performance AS (
		SELECT 
			t.customer_id,
			t.instrument_id,
			SUM(CASE WHEN t.direction = 'BUY' THEN t.execution_size ELSE 0 END) as qty_buys,
			SUM(CASE WHEN t.direction = 'SELL' THEN t.execution_size ELSE 0 END) as qty_sells,
			SUM(CASE WHEN t.direction = 'BUY' THEN t.execution_size * t.execution_price ELSE 0 END) as total_buys,
			SUM(CASE WHEN t.direction = 'SELL' THEN t.execution_size * t.execution_price ELSE 0 END) as total_sells
		FROM trades t
    	GROUP BY 1, 2),
    accounts_aggregated AS (
        SELECT
        	a.customer_id,
        	a.instrument_id,
        	SUM(a.qty_buys) AS qty_buys,
        	SUM(a.qty_sells) AS qty_sells,
        	SUM(a.total_buys) AS total_buys,
        	SUM(a.total_sells) AS total_sells,
        	SUM(a.qty_buys) - SUM(a.qty_sells) AS current_qty
        FROM accounts_performance a
        WHERE a.qty_sells <= a.qty_buys
        GROUP BY 1, 2),
    accounts_currents AS (
        SELECT
            a.customer_id,
            a.instrument_id,
            a.qty_buys,
            a.qty_sells,
            a.total_buys,
            a.total_sells,
            a.current_qty * l.latest_price AS current_value
        FROM accounts_aggregated a
        LEFT JOIN latest_price l ON(a.instrument_id = l.instrument_id)),
    accounts_snapshot AS (
        SELECT
            a.customer_id,
            i.instrument_type,
            SUM(a.qty_buys) AS qty_buys,
            SUM(a.qty_sells) AS qty_sells,
            SUM(a.total_buys) AS total_buys,
            SUM(a.total_sells) AS total_sells,
            SUM(a.current_value) AS current_value,
            SUM(a.current_value) + SUM(a.total_sells) - SUM(a.total_buys) AS return,
            100 * ((SUM(a.current_value) + SUM(a.total_sells)) / SUM(a.total_buys) - 1) AS roi
        FROM accounts_currents a
        LEFT JOIN instruments i ON(a.instrument_id = i.instrument_id)
        GROUP BY 1, 2)
SELECT
    a.customer_id,
    a.instrument_type,
    a.qty_buys,
    a.qty_sells,
    a.total_buys,
    a.total_sells,
    a.current_value,
    a.return,
    a.roi,
    p.currency AS fav_currency,
    l.latest_price AS currency_usd_price,
    a.current_value * l.latest_price AS current_value_fav_currency 
FROM accounts_snapshot a
LEFT JOIN prefixes p ON(RIGHT(a.customer_id::TEXT, 1) = p.last_digit)
LEFT JOIN latest_currencies l ON(p.currency = l.currency)
ORDER BY 1;
