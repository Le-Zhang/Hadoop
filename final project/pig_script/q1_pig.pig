AD 'keyword-all.txt' AS (file:chararray,keyword:chararray);
t2 = LOAD 'keyword-all.txt' AS (file:chararray,keyword:chararray);
C = JOIN t1 BY $1, t2 BY $1;
D = GROUP C BY ($0,$2);
Dcount = FOREACH D GENERATE FLATTEN(group) AS (ID1, ID2), (double) COUNT(C) AS match_count;
E = GROUP t1 BY $0;
Tcount = FOREACH E GENERATE group as ID, (double) COUNT(t1) AS item_count;
DFilter = FILTER Dcount BY match_count > 2;
TDcount = JOIN Tcount BY ID, DFilter BY ID1;
RESULT = FOREACH TDcount GENERATE ID1 AS ID1, ID2 AS ID2, match_count/item_count AS STRENGTH;
RESULT_2 = FILTER RESULT BY ID1 != ID2;
STORE RESULT_2 INTO 'keyword_output';

