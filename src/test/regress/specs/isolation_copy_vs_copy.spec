# Session 1 - Create hash distributed table and COPY
session "s1"

setup
{
	CREATE TABLE hash_table(id integer, data text);
	SELECT create_distributed_table('hash_table', 'id');
}

step "s1-begin" { BEGIN; }
step "s1-copy" { COPY hash_table FROM PROGRAM 'echo 1,a' WITH CSV; }
step "s1-commit" { COMMIT; }
teardown { DROP TABLE hash_table; }

# Session 2 - COPY to hash distributed table
session "s2"
step "s2-begin" { BEGIN; }
step "s2-copy" { COPY hash_table FROM PROGRAM 'echo 2,b' WITH CSV; }
step "s2-commit" { COMMIT; }
step "s2-select" { SELECT COUNT(*) FROM hash_table; }

# Session 3 - Create append distributed table and COPY
session "s3"

setup
{
	CREATE TABLE append_table(id integer, data text);
	SELECT create_distributed_table('append_table', 'id', 'append');
}

step "s3-begin" { BEGIN; }
step "s3-copy" { COPY append_table FROM PROGRAM 'echo 1,a' WITH CSV; }
step "s3-commit" { COMMIT; }
teardown { DROP TABLE append_table; }

# Session 4 - COPY to append distributed table
session "s4"
step "s4-begin" { BEGIN; }
step "s4-copy" { COPY append_table FROM PROGRAM 'echo 2,b' WITH CSV; }
step "s4-commit" { COMMIT; }
step "s4-select" { SELECT COUNT(*) FROM append_table; }

# Session 5 - Create range distributed table and COPY
session "s5"

setup
{
	CREATE TABLE range_table(id integer, data text);
	SELECT create_distributed_table('range_table', 'id', 'range');
	SELECT master_create_empty_shard('range_table');
	UPDATE pg_dist_shard SET shardminvalue = 0, shardmaxvalue = 500 WHERE logicalrelid = 'range_table'::regclass;
}

step "s5-begin" { BEGIN; }
step "s5-copy" { COPY range_table FROM PROGRAM 'echo 1,a' WITH CSV; }
step "s5-commit" { COMMIT; }
teardown { DROP TABLE range_table; }

# Session 6 - COPY to range distributed table
session "s6"
step "s6-begin" { BEGIN; }
step "s6-copy" { COPY range_table FROM PROGRAM 'echo 1,b' WITH CSV; }
step "s6-commit" { COMMIT; }
step "s6-select" { SELECT COUNT(*) FROM range_table; }

# test copy to hash vs copy to hash
permutation "s1-begin" "s2-begin" "s1-copy" "s2-copy" "s1-commit" "s2-commit" "s2-select"

# test copy to append vs copy to append
permutation "s3-begin" "s4-begin" "s3-copy" "s4-copy" "s3-commit" "s4-commit" "s4-select"

# test copy to range vs copy to range
permutation "s5-begin" "s6-begin" "s5-copy" "s6-copy" "s5-commit" "s6-commit" "s6-select"
