def getOpenConnection(user='polina', password='secret', dbname='Partitioning'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute('CREATE TABLE {0} (userid INT, movieid INT, rating FLOAT);'.format(ratingstablename))
    with open(str(ratingsfilepath), 'r') as f:
        for line in f:
            words = line.split('::')
            cur.execute('INSERT INTO {0} VALUES ({1},{2},{3});'.format(ratingstablename,words[0],words[1],words[2]))
    openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    step = 5.0/numberofpartitions
    range = [[i*step, (i+1)*step] for i in range(numberofpartitions)]
    for i in range(numberofpartitions):
        main_part='''CREATE TABLE range_part{0}(UserID INT, MovieID INT, Rating FLOAT);
        INSERT INTO range_part{0}(UserID, MovieID, Rating)
    	   SELECT UserID, MovieID, Rating
    	      FROM Ratings
    	         WHERE rating >{1} AND rating<={2};'''.format(i,range[i][0],range[i][1])
        cur.execute(main_part)
    zero_values = '''INSERT INTO range_part0(UserID, MovieID, Rating)
       SELECT UserID, MovieID, Rating
          FROM Ratings
             WHERE rating = 0;'''
    cur.execute(zero_values)
    openconnection.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    first_part = '''DROP TABLE IF EXISTS indexed_ratings;
    CREATE TABLE indexed_ratings(
	index SERIAL PRIMARY KEY,
	UserID INT,
	MovieID INT,
	Rating FLOAT
    );
    INSERT INTO indexed_ratings(userid, movieid, rating)
	   SELECT userid, movieid, rating
	   FROM {};'''.format(ratingstablename)
    cur.execute(first_part)
    for i in range(numberofpartitions):
        index = numberofpartitions - i/numberofpartitions
        rr_tables = '''CREATE TABLE rrobin_part{}(
	       UserID INT,
	          MovieID INT,
	             Rating FLOAT
                 );'''.format(i)
        cur.execute(rr_tables)
        rr_partition = '''INSERT INTO rrobin_part{0}
        SELECT userid, movieid, rating
        FROM indexed_ratings
        WHERE MOD(index-1,{1}) = {0};'''.format(i, numberofpartitions)
        cur.execute(rr_partition)
    openconnection.commit()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    insert_ratings = '''INSERT INTO {0}(userid, movieid, rating)
        VALUES({1},{2},{3});
        INSERT INTO indexed_ratings(userid, movieid, rating)
        VALUES({1},{2},{3});'''.format(ratingstablename,userid, itemid, rating)
    cur.execute(insert_ratings)
    all_table_names = '''SELECT table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
    AND table_schema='public';'''
    cur.execute(all_table_names)
    list_of_table_names = list(cur.fetchall())
    index_rrobin_list = []
    for i in list_of_table_names:
        if 'rrobin_part' in str(i):
            index_rrobin_list.append(int(i[0][-1]))
    numberofpartitions = max(index_rrobin_list)+1
    for i in range(numberofpartitions):
        rr_partition = '''INSERT INTO rrobin_part{0}
        SELECT userid, movieid, rating
        FROM indexed_ratings
        WHERE MOD(index-1,{1}) = {0}
        AND userid='{2}' AND movieid='{3}'
        AND rating='{4}';'''.format(i, numberofpartitions, userid, itemid, rating)
        cur.execute(rr_partition)
    openconnection.commit()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    insert_ratings = '''INSERT INTO {0}(userid, movieid, rating)
        VALUES({1},{2},{3});'''.format(ratingstablename,userid, itemid, rating)
    cur.execute(insert_ratings)
    all_table_names = '''SELECT table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
    AND table_schema='public';'''
    cur.execute(all_table_names)
    list_of_table_names = list(cur.fetchall())
    index_range_list = []
    for i in list_of_table_names:
        if 'range_part' in str(i):
            index_range_list.append(int(i[0][-1]))
    numberofpartitions = max(index_range_list)+1
    step = 5.0/numberofpartitions
    range = [[i*step, (i+1)*step] for i in range(numberofpartitions)]
    for i in range(numberofpartitions):
        main_part='''INSERT INTO range_part{0}(UserID, MovieID, Rating)
    	SELECT UserID, MovieID, Rating
    	FROM Ratings
    	WHERE rating >{1} AND rating<={2}
        AND userid='{3}' AND movieid='{4}'
        AND rating='{5}';'''.format(i,range[i][0],range[i][1],userid, itemid, rating)
        cur.execute(main_part)
    if rating == 0:
        zero_values = '''INSERT INTO range_part0(UserID, MovieID, Rating)
        SELECT UserID, MovieID, Rating
        FROM Ratings
        WHERE rating = 0
        AND userid='{0}' AND movieid='{1}'
        AND rating='{2}';'''.format(userid, itemid, rating)
        cur.execute(zero_values)
    openconnection.commit()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists').format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except(psycopg2.DatabaseError, e):
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except(IOError, e):
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
