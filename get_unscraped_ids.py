import sys

def get_unscraped_ids(db, n):
    db.execute("""SELECT author_name FROM
    (SELECT DISTINCT author_name FROM 
    (SELECT author_name FROM %s.comments
    UNION
    SELECT author_name FROM %s.threads) t2
    WHERE author_name NOT IN 
    (SELECT username as author_name FROM
    %s.users)
    ) t1 
    ORDER BY random() 
    LIMIT %%s""" % (db.schema, db.schema, db.schema), [n])
    return [x[0] for x in db.fetchall()]
