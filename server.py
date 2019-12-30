import flask
from flask import request
import pandas
import os
import re
import urllib.parse
import sqlite3
import codecs

#csv_file = open(os.path.join(os.getcwd(), 'sample_csv.csv'))
df = pandas.read_csv(codecs.open(os.path.join(os.getcwd(), 'sample_csv.csv'), encoding='utf-8'))

print(df)

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Query Engine</h1><p>Please input your query as /query?** where ** are well formatted conditions </p>"


@app.route('/query', methods=['GET'])
def api_filter():
    query_string_byte = request.query_string
    query_string = urllib.parse.unquote(str(query_string_byte)[2:-1])

    query_string_temp = query_string.replace(' and ', '%')
    query_string_temp = query_string_temp.replace(' or ', '%')
    parsed_conditions = query_string_temp.split('%')

    list_AND = [m.start() for m in re.finditer(' and ', query_string)]
    list_OR = [m.start() for m in re.finditer(' or ', query_string)]

    query = "SELECT * FROM sample_table WHERE "

    for condition in parsed_conditions:
        if '$=' in condition:
            parsed = condition.split(' $= ')
            query += parsed[0] + '="' + parsed[1] + '" COLLATE NOCASE '
        elif '!=' in condition:
            parsed = condition.split(' != ')
            query += parsed[0] + '<>"' + parsed[1] + '" '
        elif '==' in condition:
            parsed = condition.split(' == ')
            query += parsed[0] + '="' + parsed[1] + '" '
        elif '&=' in condition:
            parsed = condition.split(' &= ')
            query += parsed[0] + ' LIKE ' + '"%' + parsed[1] + '%" '

        if len(list_AND) == 0 and len(list_OR) == 0:
            continue
        elif len(list_AND) != 0 and len(list_OR) == 0:
            query += 'AND '
            list_AND.pop(0)
        elif len(list_AND) == 0 and len(list_OR) != 0:
            query += 'OR '
            list_OR.pop(0)
        else:
            if list_AND[0] < list_OR[0]:
                query += 'AND '
                list_AND.pop(0)
            else:
                query += 'OR '
                list_OR.pop(0)

    conn = sqlite3.connect('sample_csv.db')
    cur = conn.cursor()
    df.to_sql(name='sample_table', con=conn, if_exists='replace', index=False)
    cur.execute(query)

    query_df = pandas.DataFrame(cur.fetchall(), columns = list(df))
    print(query_df)
    return query_df.to_html()

app.run()