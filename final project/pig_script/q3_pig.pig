AD 'author-all.txt' AS (id1:chararray, auth1:chararray);
t2 = LOAD 'author-all.txt' AS (id2:chararray, auth2:chararray);
t3 = JOIN t1 BY auth1, t2 BY auth2;
t3 = FILTER t3 BY id1 != id2;
t3 = GROUP t3 by (id1,id2);
t3 = FOREACH t3 GENERATE group.id1,group.id2;
STORE t3 INTO 'author_output';

