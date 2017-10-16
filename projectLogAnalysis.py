import datetime
import psycopg2
import bleach


def execute_query(query):
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(query)
    result = c.fetchall()
    db.close()
    return result


def most_popular_articles():
    # 1. What are the most popular three articles of all time? Which articles
    # have been accessed the most? Present this information as a sorted list
    # with the most popular article at the top.

    articles = execute_query(
        """select articles.title, count(log.ip) as views from articles,
         log where path=CONCAT('/article/', articles.slug)
         group by articles.title order by views desc limit 3;""")
    print("Top 3 amost read articles")
    for title, views in articles:
        print("%s - %d views" % (title, views))


def most_popular_author():
    # 2. Who are the most popular article authors of all time? That is, when
    # you sum up all of the articles each author has written, which authors
    # get the most page views? Present this as a sorted list with the most
    # popular author at the top.
    print("\nTop 3 most popular Authors")
    authors = execute_query(
        """select authors.name, count(ip) as views from articles
        join log  on path = CONCAT('/article/', articles.slug)
        join authors  on authors.id = articles.author
        group by authors.name order by views DESC limit 3""")
    for author, views in authors:
        print("%s - %d views" % (author, views))


def days_more_than_1_percent_errors():
    # On which days did more than 1% of requests lead to errors? The log table
    # includes a column status that indicates the HTTP status code that the
    # news site sent to the user's browser. (Refer back to this lesson if you
    # want to review the idea of HTTP status codes.)
    print("\nDays where error views more than 1% from success views")
    agregatedData = execute_query("""select suc.total, err.total, suc.dato
        from (select count(path) as total, cast (time as DATE) as dato
        from log where status='200 OK' group by dato) as suc,
        (select count(path) as total, cast (time as DATE) as dato
        from log where status!='200 OK' group by dato) as err
        where suc.dato=err.dato""")
    for successViews, errViews, date in agregatedData:
        if(int(errViews) / int(successViews) > 0.01):
            print("%s - %10.2f %%errors" %
                  (date, (int(errViews) / int(successViews)) * 100))

if __name__ == '__main__':
    most_popular_articles()
    most_popular_author()
    days_more_than_1_percent_errors()
