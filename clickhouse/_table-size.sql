SELECT
    database, `table`,
    sum(rows) AS rows,
--     formatReadableQuantity(count()) AS parts,
sum(data_uncompressed_bytes) AS data_size_uncompressed,
sum(data_compressed_bytes) AS data_size_compressed,
sum(bytes_on_disk) AS total_size_on_disk
FROM system.parts
WHERE active  AND (`table` = 'otel_logs')
GROUP BY database, `table`
ORDER BY sum(bytes_on_disk) ASC
FORMAT JSONEachRow;


SELECT
    database, `table`,
    sum(primary_key_size) + sum(marks_bytes) AS size_sparse_primary_index
FROM system.parts
WHERE active  AND (`table` = 'otel_logs')
GROUP BY database, `table`
ORDER BY sum(bytes_on_disk) ASC
FORMAT JSONEachRow;


SELECT
    database, `table`,
    sum(secondary_indices_compressed_bytes) + sum(secondary_indices_marks_bytes)  AS size_inverted_index
FROM system.parts
WHERE active  AND (`table` = 'otel_logs')
GROUP BY database, `table`
ORDER BY sum(bytes_on_disk) ASC
FORMAT JSONEachRow;









