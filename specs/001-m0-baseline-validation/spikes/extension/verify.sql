.bail off
LOAD '/tmp/ensabc-m0-extension-target/ensabc_m0_extension.duckdb_extension';

SELECT CASE WHEN count(*) = 2 AND bool_and(ens_m0_echo(members) = members)
    THEN 'PASS: task_id stays outside the UDF; distinct legal tasks round-trip'
    ELSE error('round-trip or task isolation failed')
END
FROM (VALUES
    ('task-a', [{'member_id': 'm1', 'features': [1.5, 2.0]}, {'member_id': 'm2', 'features': [3.0, 4.0]}]),
    ('task-b', [{'member_id': 'm7', 'features': [9.0, 8.0, 7.0]}])
) AS tasks(task_id, members);

SELECT CASE WHEN ens_m0_echo(NULL::STRUCT(member_id VARCHAR, features DOUBLE[])[]) IS NULL
    THEN 'BLOCKED: top-level NULL returned NULL without rejection; FR-009 failure under default NULL propagation'
    ELSE error('top-level NULL unexpectedly changed behavior')
END;

SELECT CASE WHEN ens_m0_echo(members) = members
    THEN 'BLOCKED: echo accepts unequal feature lengths within one task; this is formal-contract-invalid input'
    ELSE error('unequal-length observation changed')
END
FROM (
    SELECT [
        struct_pack(member_id := concat('m1-', i::VARCHAR), features := [1.0, 2.0]),
        struct_pack(member_id := concat('m2-', i::VARCHAR), features := [3.0])
    ] AS members
    FROM range(1) AS rows(i)
);

SELECT CASE WHEN count(*) = 4 AND bool_and(rejected)
    THEN 'OBSERVED: TRY returned NULL for four nested cases; this alone proves neither raw query failure nor session recovery'
    ELSE error('a nested NULL was not caught by TRY')
END
FROM (VALUES
    (TRY(ens_m0_echo([NULL::STRUCT(member_id VARCHAR, features DOUBLE[])])) IS NULL),
    (TRY(ens_m0_echo([struct_pack(member_id := NULL::VARCHAR, features := [1.0])])) IS NULL),
    (TRY(ens_m0_echo([struct_pack(member_id := 'm', features := NULL::DOUBLE[])])) IS NULL),
    (TRY(ens_m0_echo([struct_pack(member_id := 'm', features := [NULL::DOUBLE])])) IS NULL)
) AS invalid_cases(rejected);

SELECT CASE WHEN count(*) = 2100 AND bool_and(ens_m0_echo(members) = members)
    THEN 'PASS: 2100 varying rows fully consumed across vector chunks'
    ELSE error('multi-chunk round-trip failed')
END
FROM (
    SELECT i, [struct_pack(member_id := i::VARCHAR, features := [i::DOUBLE, (i + 0.5)::DOUBLE])] AS members
    FROM range(2100) AS rows(i)
);

SELECT CASE WHEN array_length(ens_m0_echo(members)) = 3000 AND ens_m0_echo(members) = members
    THEN 'PASS: one list with 3000 members and two features per member round-trips'
    ELSE error('large member list round-trip failed')
END
FROM (
    SELECT list_transform(range(0, 3000), lambda i: struct_pack(
        member_id := i::VARCHAR,
        features := [i::DOUBLE, (i + 0.5)::DOUBLE]
    )) AS members
);

-- This query must fail without TRY; the next statement records same-session recovery if the CLI continues.
SELECT ens_m0_echo([
    struct_pack(member_id := concat('bad-', i::VARCHAR), features := [NULL::DOUBLE])
])
FROM range(2) AS invalid_rows(i);

SELECT CASE WHEN count(*) = 2 AND bool_and(ens_m0_echo(members) = members)
    THEN 'PASS: same CLI session completed valid calls after an unhandled nested-NULL query error'
    ELSE error('same-session recovery after the raw query error failed')
END
FROM (
    SELECT i, [struct_pack(member_id := concat('after-error-', i::VARCHAR), features := [i::DOUBLE, (i + 1)::DOUBLE])] AS members
    FROM range(2) AS rows(i)
);
