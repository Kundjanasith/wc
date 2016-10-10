from flask import Flask, render_template, jsonify
from contextlib import contextmanager
from pyspark import SparkContext
from pyspark import SparkConf
from operator import add
from pyspark.sql import *
import json
import ast
import re

useless = [u'\u0e41\u0e25\u0e30', u'\u0e17\u0e31\u0e49\u0e07', u'\u0e01\u0e47', u'\u0e04\u0e23\u0e31\u0e49\u0e19', u'\u0e08\u0e36\u0e07', u'\u0e01\u0e47\u0e14\u0e35', u'\u0e40\u0e21\u0e37\u0e48\u0e2d', u'\u0e01\u0e47\u0e27\u0e48\u0e32', u'\u0e1e\u0e2d', u'\u0e41\u0e25\u0e49\u0e27', u'\u0e01\u0e32\u0e23', u'\u0e41\u0e15\u0e48', u'\u0e41\u0e15\u0e48\u0e27\u0e48\u0e32', u'\u0e01\u0e27\u0e48\u0e32', u'\u0e16\u0e36\u0e07', u'\u0e44\u0e21\u0e48', u'\u0e2b\u0e23\u0e37\u0e2d', u'\u0e17\u0e35\u0e48', u'\u0e43\u0e2b\u0e49', u'\u0e40\u0e1b\u0e47\u0e19', u'\u0e02\u0e2d\u0e07', u'\u0e21\u0e35', u'\u0e04\u0e13\u0e30', u'\u0e01\u0e23\u0e23\u0e21\u0e01\u0e32\u0e23', u'\u0e1e.\u0e28', u'\u0e23\u0e48\u0e32\u0e07', u'\u0e04\u0e27\u0e32\u0e21', u'\u0e40\u0e1e\u0e37\u0e48\u0e2d', u'\u0e40\u0e23\u0e37\u0e48\u0e2d\u0e07', u'.', u' ', u'\u0e43\u0e19', u')', u'(', u'\u0e15\u0e32\u0e21', u'\u0e44\u0e14\u0e49', u'\u0e1c\u0e39\u0e49', u'\u0e01\u0e33\u0e2b\u0e19\u0e14', u'\u0e15\u0e31\u0e49\u0e07', u'\u0e01\u0e33\u0e2b\u0e19\u0e14', u'\u0e44\u0e1b', u'\u0e42\u0e14\u0e22', u'\u0e41\u0e2b\u0e48\u0e07', u'\u0e15\u0e48\u0e2d', u'*', u'\u0e43\u0e0a\u0e49', u'\u0e23\u0e32\u0e0a', u'\u0e01\u0e31\u0e1a', u'\u0e27\u0e48\u0e32', u'\u0e41\u0e15\u0e48\u0e07', u'\u0e17\u0e32\u0e07', u'\u0e14\u0e49\u0e27\u0e22', u'\u0e44\u0e21\u0e48\u0e40\u0e0a\u0e48\u0e19\u0e19\u0e31\u0e49\u0e19', u'\u0e08\u0e36\u0e07', u'\u0e40\u0e1e\u0e23\u0e32\u0e30', u'-', u'\u0e23\u0e48\u0e27\u0e21', u'\u0e27\u0e31\u0e19', u'\u0e23\u0e31\u0e1a', u'\u0e07\u0e32\u0e19', u'\u0e21\u0e15\u0e34', u'\u0e23\u0e30\u0e2b\u0e27\u0e48\u0e32\u0e07', u'\u0e2b\u0e25\u0e31\u0e01', u'\u0e0a\u0e2d\u0e1a', u'\u0e2a\u0e48\u0e07', u'\u0e08\u0e32\u0e01', u'\u0e2d\u0e2d\u0e01', u'\u0e19\u0e32\u0e22', u'\u0e1b\u0e35', u'\u0e08\u0e31\u0e14', u'\u0e1b\u0e4c', u'\u0e08\u0e31\u0e14', u'\u0e15\u0e48\u0e32\u0e07', u'\u0e0b\u0e36\u0e48\u0e07', u'\u0e09\u0e1a\u0e31\u0e1a', u'\u0e15\u0e49\u0e2d\u0e07', u'\u0e2a\u0e48\u0e27\u0e19\u0e1c\u0e25', u'\u0e01\u0e0f', u'\u0e08\u0e30', u'\u0e1e\u0e23\u0e30', u'\u0e06\u0e48\u0e32', u'\u0e04\u0e23\u0e31\u0e1a', u'\u0e1c\u0e25', u'\u0e02\u0e2d', u'\u0e23\u0e48\u0e27\u0e21', u'\u0e23\u0e27\u0e21', u'\u0e21\u0e37\u0e2d', u'\u0e14\u0e31\u0e07', u'\u0e04\u0e19']
r = re.compile(u'[\u0E01-\u0E2E]+')



SPARK_MASTER='local'
SPARK_APP_NAME='Word Count'
SPARK_EXECUTOR_MEMORY='200m'

app = Flask(__name__)

@contextmanager
def spark_manager():
    conf = SparkConf().setMaster(SPARK_MASTER) \
                      .setAppName(SPARK_APP_NAME) \
                      .set("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    spark_context = SparkContext(conf=conf)

    try:
        yield spark_context
    finally:
        spark_context.stop()

import PyICU

def isThai(chr):
    cVal = ord(chr)
    if(cVal >= 3584 and cVal <= 3711):
        return True

    return False

def wrap(txt):
  txt = PyICU.UnicodeString(txt)
  bd = PyICU.BreakIterator.createWordInstance(PyICU.Locale("th"))
  bd.setText(txt)
  lastPos = bd.first()
  retTxt = PyICU.UnicodeString("")
  txt_list = []
  try:
    while(1):
      currentPos = bd.next()
      retTxt += txt[lastPos:currentPos]
      #
      txt_list.append(txt[lastPos:currentPos])
      #Only thai language evaluated
      if(isThai(txt[currentPos-1])):
        if(currentPos < len(txt)):
          if(isThai(txt[currentPos])):
            #This is dummy word seperator
            #retTxt += PyICU.UnicodeString("|||")
            #
            pass
      lastPos = currentPos
  except StopIteration:
    pass
    #retTxt = retTxt[:-1]
  #return retTxt
  return [unicode(i) for i in txt_list]


def fullwrap(txt):
    txt_list = txt.split(' ')
    new_list = []
    for i in txt_list:
        #new_list.extend(wrap(i).split('|||'))
        new_list.extend(wrap(i))

    return new_list

@app.route('/searchdata', methods=['POST'])
def searchdata():
    search =  request.form['search']
    return render_template("cloud.html", search)


@app.route("/")
def index():
    with spark_manager() as context:
        data = context.textFile("hdfs:///user/zeppelin/*.txt")
        dataFinal = data.flatMap(fullwrap).filter(lambda x : x not in useless and r.match(x)).map(lambda x : (x, 1)).reduceByKey(add)
        sqlContext = SQLContext(context)
        df = sqlContext.createDataFrame(dataFinal, ['text', 'size'])
        df2 = df.sort(df.size, ascending=False)
        tmp_list = df2.toJSON().map(lambda x:ast.literal_eval(x)).take(100)
        tmp_list2 = dataFinal.sortBy(lambda x:x[1], ascending=False).take(10)
	max_value = dataFinal.max(lambda x : x[1])[1]
    return render_template("index.html", word_data = tmp_list, word_data2 = tmp_list2, max_value=max_value)
if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
