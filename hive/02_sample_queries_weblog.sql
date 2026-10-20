-- Sample queries (columns are defined by RegexSerDe in CREATE TABLE)

USE demo_logs;

SELECT COUNT(*) AS log_lines FROM weblog;

SELECT * FROM weblog LIMIT 10;

SELECT client_ip, method, path, status, log_datetime
FROM weblog
LIMIT 10;

SELECT status, COUNT(*) AS requests
FROM weblog
GROUP BY status
ORDER BY requests DESC;

SELECT client_ip, method, path, status
FROM weblog
WHERE status IN ('403', '404', '500', '502', '503')
ORDER BY status
LIMIT 20;
