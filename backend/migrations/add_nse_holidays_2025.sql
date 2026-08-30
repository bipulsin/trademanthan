-- NSE closed dates for 2025 (IST calendar). Idempotent seed for backtest trading-day accuracy.
INSERT INTO holiday (holiday_date, description) VALUES
    ('2025-02-26', 'Mahashivratri'),
    ('2025-03-14', 'Holi'),
    ('2025-03-31', 'Id-Ul-Fitr (Ramadan Eid)'),
    ('2025-04-10', 'Shri Mahavir Jayanti'),
    ('2025-04-14', 'Dr. Baba Saheb Ambedkar Jayanti'),
    ('2025-04-18', 'Good Friday'),
    ('2025-05-01', 'Maharashtra Day'),
    ('2025-08-15', 'Independence Day'),
    ('2025-08-27', 'Ganesh Chaturthi'),
    ('2025-10-02', 'Mahatma Gandhi Jayanti/Dussehra'),
    ('2025-10-21', 'Diwali Laxmi Pujan'),
    ('2025-10-22', 'Diwali-Balipratipada'),
    ('2025-11-05', 'Prakash Gurpurb Sri Guru Nanak Dev'),
    ('2025-12-25', 'Christmas')
ON CONFLICT (holiday_date) DO NOTHING;
