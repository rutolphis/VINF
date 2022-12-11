import xml.etree.ElementTree as ET
import re
import json
from deep_translator import GoogleTranslator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

def translation(input):
    translatedEs = GoogleTranslator(source='sk', target='es').translate(text=input)
    translatedEn = GoogleTranslator(source='sk', target='en').translate(text=input)
    print("Spanish: " + translatedEs)
    print("English: " + translatedEn)

    return [input, translatedEn, translatedEs]

def menu(input):
    if input == '1':
            parse_data()

    elif input == '2':
            load_data()

    else:
            return 0

def parse_data():
    patternPage = r"(?<=<page>)(.*?)(?=<\/page>)"
    patternTitle = r"(?<=<title>)(.*?)(?=<\/title>)"
    patternText = r"<text.*?>(.*?)</text>"
    patternTextDelete = r">([^<]*)<"
    patternContributorUsername = r"(?<=<username>)(.*?)(?=<\/username>)"
    patternContributorIp = r"(?<=<ip>)(.*?)(?=<\/ip>)"

    pages = {}

    with open('/Users/zvjssnn01zvjssnn01/development/school/VINF/data/test.xml', 'r', encoding='utf-8') as f:  #'C:\SCHOOL\VINF\data\skwiki-20221001-pages-articles-multistream.xml'
        data = f.read()
        result = re.findall(patternPage, data,re.DOTALL)

        for i in result:
            title = re.search(patternTitle,i,re.DOTALL)
            text = re.search(patternText,i,re.DOTALL).group(0)
            text = re.search(patternTextDelete, text, re.DOTALL)
            username = re.search(patternContributorUsername, i, re.DOTALL)
            ip = re.search(patternContributorIp, i, re.DOTALL)
            if title is not None:
                content = {}
                if text is not None:
                    content['text'] = text.group(0).lstrip(text.group(0)[0]).rstrip(text.group(0)[-1])
                else:
                    content['text'] = None

                if username is not None:
                    content['username'] = username.group(0)
                else:
                    content['username'] = None

                if ip is not None:
                    content['ip'] = ip.group(0)
                else:
                    content['ip'] = None

                pages[title.group(0)] = content

    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(pages, f, ensure_ascii=False, indent=4)

    load_spark()
    #search(pages)


def search(dataset):
    print("Input what you want to search:")
    search_string = input()

    searcher_strings = translation(search_string)
    occurrences = []

    for i in range(0, len(searcher_strings)):
        for key, value in dataset.items():
            if searcher_strings[i] in key or search_string in value['text'] :
                occurrences.append(value)

    print("Results:")
    print(occurrences)

def load_spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("PySpark Read JSON") \
        .getOrCreate()

    # Reading JSON file into dataframe
    dataframe = spark.read.option("multiline", "true") \
        .json("/Users/zvjssnn01zvjssnn01/development/school/VINF/data.json")
    dataframe.printSchema()
    dataframe.show()

    # Reading multiline json file

    # Writing PySpark dataframe into JSON File
    dataframe.write.mode('Overwrite').json("/Users/zvjssnn01zvjssnn01/development/school/VINF/output.json")


def load_data():
    with open('data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        search(data)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("Choose option:\n"
          "1 - parse data and search\n"
          "2 - load data and search\n"
          "3 - parse data")
    x = input()
    menu(x)


