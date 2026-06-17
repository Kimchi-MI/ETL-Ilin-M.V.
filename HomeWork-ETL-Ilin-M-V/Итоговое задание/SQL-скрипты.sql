CREATE TABLE transactions_v2
(
    call_id Utf8,
    call_time Timestamp,
    client_id Utf8,
    region_code Utf8,
    campaign_type Utf8,
    call_status Utf8,
    client_response Utf8,
    duration_sec Uint32,
    follow_up_required Bool,
    PRIMARY KEY (call_id)
);

REPLACE INTO transactions_v2 (call_id, call_time, client_id, region_code, campaign_type, call_status, client_response, duration_sec, follow_up_required) VALUES
    ('call_20260501_001', CAST(Timestamp('2026-05-01T11:42:15Z') AS Timestamp), 'client_4412', 'DE-HE', 'credit_card_offer', 'answered', 'interested', 184, true),
    ('call_20260501_002', CAST(Timestamp('2026-05-01T12:15:30Z') AS Timestamp), 'client_7890', 'US-CA', 'product_upgrade', 'no_answer', NULL, 0, false),
    ('call_20260501_003', CAST(Timestamp('2026-05-01T14:22:45Z') AS Timestamp), 'client_1123', 'GB-LDN', 'service_renewal', 'answered', 'not_interested', 95, false),
    ('call_20260501_004', CAST(Timestamp('2026-05-01T15:30:00Z') AS Timestamp), 'client_5567', 'FR-PAR', 'credit_card_offer', 'answered', 'interested', 210, true),
    ('call_20260501_005', CAST(Timestamp('2026-05-01T16:45:20Z') AS Timestamp), 'client_9988', 'DE-BY', 'product_upgrade', 'answered', 'callback_request', 67, true),
    ('call_20260501_006', CAST(Timestamp('2026-05-01T17:15:45Z') AS Timestamp), 'client_3344', 'US-NY', 'loan_offer', 'busy', NULL, 0, false),
    ('call_20260501_007', CAST(Timestamp('2026-05-01T18:30:10Z') AS Timestamp), 'client_7766', 'IT-ROM', 'insurance_sale', 'answered', 'purchase', 145, false),
    ('call_20260501_008', CAST(Timestamp('2026-05-01T19:45:55Z') AS Timestamp), 'client_2233', 'ES-MAD', 'credit_card_offer', 'answered', 'not_interested', 78, false),
    ('call_20260501_009', CAST(Timestamp('2026-05-01T20:55:30Z') AS Timestamp), 'client_6677', 'DE-HE', 'product_upgrade', 'answered', 'not_interested', 78, false),
    ('call_20260501_010', CAST(Timestamp('2026-05-01T21:10:15Z') AS Timestamp), 'client_8899', 'GB-LDN', 'service_renewal', 'answered', 'interested', 320, true);

    REPLACE INTO transactions_v2 (call_id, call_time, client_id, region_code, campaign_type, call_status, client_response, duration_sec, follow_up_required)
SELECT
    CAST(CONCAT('call_20260501_', CAST(100000 + ROW_NUMBER() OVER () AS Utf8)) AS Utf8),
    CAST(Timestamp('2026-05-01T10:00:00Z') AS Timestamp),
    CAST(CONCAT('client_', CAST(1000 + (ROW_NUMBER() OVER () % 9000) AS Utf8)) AS Utf8),
    CAST(['DE-HE', 'US-CA', 'GB-LDN', 'FR-PAR', 'DE-BY', 'US-NY', 'IT-ROM', 'ES-MAD'][ROW_NUMBER() OVER () % 8] AS Utf8),
    CAST(['credit_card_offer', 'product_upgrade', 'service_renewal', 'loan_offer', 'insurance_sale'][ROW_NUMBER() OVER () % 5] AS Utf8),
    CAST(['answered', 'no_answer', 'busy', 'wrong_number'][ROW_NUMBER() OVER () % 4] AS Utf8),
    CASE 
        WHEN ['answered', 'no_answer', 'busy', 'wrong_number'][ROW_NUMBER() OVER () % 4] = 'answered' 
        THEN CAST(['interested', 'not_interested', 'callback_request', 'purchase'][ROW_NUMBER() OVER () % 4] AS Utf8)
        ELSE NULL 
    END,
    CAST(
        CASE 
            WHEN ['answered', 'no_answer', 'busy', 'wrong_number'][ROW_NUMBER() OVER () % 4] = 'answered' 
            THEN 30 + (ROW_NUMBER() OVER () % 270)
            ELSE 0 
        END AS Uint32
    ),
    CAST(
        CASE 
            WHEN ['answered', 'no_answer', 'busy', 'wrong_number'][ROW_NUMBER() OVER () % 4] = 'answered' 
            THEN (ROW_NUMBER() OVER () % 2 = 0)
            ELSE false 
        END AS Bool
    )
FROM (SELECT 1 FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) a
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) b
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) c
    CROSS JOIN (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) d
    CROSS JOIN (SELECT 1 UNION SELECT 2) e) t;
