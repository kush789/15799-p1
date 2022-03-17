#!/bin/sh

# Figure out existing indexes without constraints
echo "select idx.relname as index_name
    from pg_index pgi
    join pg_class idx on idx.oid = pgi.indexrelid
    join pg_namespace insp on insp.oid = idx.relnamespace
    join pg_class tbl on tbl.oid = pgi.indrelid
    join pg_namespace tnsp on tnsp.oid = tbl.relnamespace
    join pg_indexes pgis on pgis.indexname = idx.relname
    where not pgi.indisunique and 
            not pgi.indisprimary and 
            not pgi.indisexclusion 
            and tnsp.nspname = 'public';" \
    | sudo -u postgres psql project1db  \
    | tail -n +3 \
    | head -n -2 \
    | awk '{ print "DROP INDEX" $0 ";"}' \
    > drop_existing_indices.sql;

# Just for debug
cat drop_existing_indices.sql;

# drop all existing indices without constraints
cat drop_existing_indices.sql | sudo -u postgres psql project1db;

cat configure_system.sql | sudo -u postgres psql project1db;