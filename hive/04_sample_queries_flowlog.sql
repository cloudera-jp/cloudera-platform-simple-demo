-- Sample queries for demo_logs.flowlog (parsed columns via RegexSerDe)

USE demo_logs;

SELECT COUNT(*) AS flow_records FROM flowlog;

SELECT * FROM flowlog LIMIT 10;

SELECT action, COUNT(*) AS records
FROM flowlog
GROUP BY action
ORDER BY records DESC;

SELECT dstport, COUNT(*) AS records
FROM flowlog
GROUP BY dstport
ORDER BY records DESC
LIMIT 10;

-- Flows involving a known private IP (example for incident triage)
SELECT srcaddr, dstaddr, dstport, action, packets, bytes
FROM flowlog
WHERE srcaddr = '10.1.1.40' OR dstaddr = '10.1.1.40'
LIMIT 20;
