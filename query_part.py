import psycopg2
import os
import sys

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    all_table_names = '''SELECT table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
        AND table_schema='public';'''
    cur.execute(all_table_names)
    list_of_table_names = list(cur.fetchall())
    index_range_list = []
    for i in list_of_table_names:
        if 'rangeratingspart' in str(i):
            index_range_list.append(int(i[0][-1]))
    numberofpartitions = max(index_range_list)+1
    step = 5.0/numberofpartitions
    range = [[i*step, (i+1)*step] for i in xrange(numberofpartitions)]
    indexes = []
    for i in range:
        if ratingMinValue >= i[0] or ratingMinValue <= i[1]:
            indexes.append(range.index(i))
        elif ratingMaxValue >= i[0] or ratingMaxValue <= i[1]:
            indexes.append(range.index(i))
    for i in indexes:
        addrows = '''SELECT*
            FROM rangeratingspart{}
            WHERE rating >= {}
            AND rating <= {};'''.format(i, ratingMinValue, ratingMaxValue)
        cur.execute(addrows)
        tuple_list = list(cur.fetchall())
        for j in tuple_list:
            a = list(j)
            a.insert(0,'RangeRatingsPart{}'.format(i))
            tuple_list[tuple_list.index(j)]= a
        writeToFile('RangeQueryOut.txt', tuple_list)
    for i in list_of_table_names:
        if 'roundrobinratingspart' in i[0]:
            addrobinrows = '''SELECT*
            FROM {}
            WHERE rating >= {}
            AND rating <= {};'''.format(i[0], ratingMinValue, ratingMaxValue)
            cur.execute(addrobinrows)
            tuple_list_robin = list(cur.fetchall())
            for j in tuple_list_robin:
                a = list(j)
                a.insert(0,'RoundRobinRatingsPart{}'.format(i[0][-1]))
                tuple_list_robin[tuple_list_robin.index(j)]= a
            writeToFile('RangeQueryOut.txt', tuple_list_robin)
    openconnection.commit()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    all_table_names = '''SELECT table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
        AND table_schema='public';'''
    cur.execute(all_table_names)
    list_of_table_names = list(cur.fetchall())
    index_range_list = []
    for i in list_of_table_names:
        if 'rangeratingspart' in str(i):
            index_range_list.append(int(i[0][-1]))
    numberofpartitions = max(index_range_list)+1
    step = 5.0/numberofpartitions
    range = [[i*step, (i+1)*step] for i in xrange(numberofpartitions)]
    indexes = []
    for i in range:
        if i[0]<=ratingValue and i[1]>=ratingValue:
            indexes.append(range.index(i))
    for i in indexes:
        addrows = '''SELECT*
            FROM rangeratingspart{}
            WHERE rating = {};'''.format(i, ratingValue)
        cur.execute(addrows)
        tuple_list = list(cur.fetchall())
        for j in tuple_list:
            a = list(j)
            a.insert(0,'RangeRatingsPart{}'.format(i))
            tuple_list[tuple_list.index(j)]= a
        writeToFile('PointQueryOut.txt', tuple_list)
    for i in list_of_table_names:
        if 'roundrobinratingspart' in i[0]:
            addrobinrows = '''SELECT*
            FROM {}
            WHERE rating = {};'''.format(i[0], ratingValue)
            cur.execute(addrobinrows)
            tuple_list_robin = list(cur.fetchall())
            for j in tuple_list_robin:
                a = list(j)
                a.insert(0,'RoundRobinRatingsPart{}'.format(i[0][-1]))
                tuple_list_robin[tuple_list_robin.index(j)]= a
            writeToFile('PointQueryOut.txt', tuple_list_robin)
    openconnection.commit()


def writeToFile(filename, rows):
    f = open(filename, 'a')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
