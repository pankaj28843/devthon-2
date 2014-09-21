import multiprocessing
from collections import OrderedDict
import datetime
from StringIO import StringIO

import requests
from html2text import html2text, HTML2Text
from lxml import etree

import xlsxwriter


DEFAULT_PROCESSES_COUNT = int(1.5 * multiprocessing.cpu_count())


def spawn(f):
    def fun(q_in, q_out):
        while True:
            i, x = q_in.get()
            if i is None:
                break
            q_out.put((i, f(x)))
    return fun


def parmap(f, X, nprocs=DEFAULT_PROCESSES_COUNT):
    q_in = multiprocessing.Queue(1)
    q_out = multiprocessing.Queue()

    proc = [multiprocessing.Process(target=spawn(f), args=(q_in, q_out))
            for _ in range(nprocs)]
    for p in proc:
        p.daemon = True
        p.start()

    sent = [q_in.put((i, x)) for i, x in enumerate(X)]
    [q_in.put((None, None)) for _ in range(nprocs)]
    res = [q_out.get() for _ in range(len(sent))]

    [p.join() for p in proc]

    return [x for i, x in sorted(res)]


def element_to_html(element):
    return u''.join(map(etree.tounicode, element))


def element_to_text(element):
    return html2text(element_to_html(element))


def process_html_using_lxml(html):
    root = etree.HTML(html)
    body = root.find(".//body")
    new_html = u''.join(map(etree.tounicode, body))
    return new_html


BASE_URL = "http://agrimarketing.telangana.gov.in/rep.jsp"
# RESOURCE_ID = "9ef84268-d588-465a-a308-a864a43d0070"
# API_KEY="88ccf84348afa53b2cb4e37f062b2d2b"


parser = etree.HTMLParser()
table_xpath = '//*[@id="table92"]/tr/td/table[2]/tr/td/div/center/table'


def parse_table(page_content):
    root = etree.parse(StringIO(page_content), parser)
    table = root.xpath(table_xpath)[0]

    dt = []
    for row in table.getchildren():
        dt.append(
            map(
                lambda x: ("".join(x.itertext())).strip(),
                row.getchildren()))
    return dt


def get_data_for_date(date_string):
    req = requests.post(
        BASE_URL,
        {
            "ARR_DATE": date_string
        }
    )

    fixed_html = re.sub(r"</tr>\s+<td", "</tr><tr><td", req.text)

    district_dict = {}
    data = parse_table(fixed_html)
    district_name = None
    market = None

    i = 1
    for row in data[1:]:
        print "row number", i
        i += 1
        if len(row) < 10:
            continue

        if row[0] != "''":
            district_name = row[0]

        if row[1] != "''":
            market = row[1]

        district_data = district_dict.get(district_name)
        if district_data is None:
            district_data = district_dict[district_name] = []

        _data = {
            'DISTRICT': district_name,
            'MARKET': market,
            'COMMODITY': row[2],
            'VARIETY': row[3],
            'ARRIVALS': row[4],
            'UNITS': row[5],
            'MIN PRICE': row[6],
            'MAX PRICE': row[7],
            'MODAL PRICE': row[8],
            'Unit of Price': row[9],
        }

        district_data.append(_data)
    return district_dict


start_date = datetime.date.today() - datetime.timedelta(days=370)
date_data = OrderedDict()
date_strings = map(
    lambda i: (start_date + datetime.timedelta(days=i)).strftime("%d-%m-%Y"), range(365))
date_data = zip(date_strings,  parmap(get_data_for_date, date_strings, 20))


def export_date_wise_data_to_xlsx(date_wise_data):
    # create a workbook in memory
    output = StringIO()

    workbook = xlsxwriter.Workbook(output, {"in_memory": True})

    worksheet = workbook.add_worksheet()

    row_index = 0

    worksheet.write(row_index, 0, 'DATE')
    worksheet.write(row_index, 1, 'DISTRICT')
    worksheet.write(row_index, 2, 'MARKET')
    worksheet.write(row_index, 3, 'COMMODITY')
    worksheet.write(row_index, 4, 'VARIETY')
    worksheet.write(row_index, 5, 'ARRIVALS')
    worksheet.write(row_index, 6, 'UNITS')
    worksheet.write(row_index, 7, 'MIN PRICE')
    worksheet.write(row_index, 8, 'MAX PRICE')
    worksheet.write(row_index, 9, 'MODAL PRICE')
    worksheet.write(row_index, 10, 'Unit of Price')

    row_index = 1

    for date_string, date_data in date_wise_data:
        for district_name, district_data in date_data.iteritems():
            for row_dict in district_data:
                worksheet.write(row_index, 0, date_string)
                worksheet.write(row_index, 1, district_name.lower())
                worksheet.write(row_index, 2, row_dict['MARKET'])
                worksheet.write(row_index, 3, row_dict['COMMODITY'])
                worksheet.write(row_index, 4, row_dict['VARIETY'])
                worksheet.write(row_index, 5, row_dict['ARRIVALS'])
                worksheet.write(row_index, 6, row_dict['UNITS'])
                worksheet.write(row_index, 7, row_dict['MIN PRICE'])
                worksheet.write(row_index, 8, row_dict['MAX PRICE'])
                worksheet.write(row_index, 9, row_dict['MODAL PRICE'])
                worksheet.write(row_index, 10, row_dict['Unit of Price'])

                row_index += 1

    return output

with open("/tmp/data.xlsx", 'wb') as f:
    buf = export_date_wise_data_to_xlsx(date_data)
    f.write(buf.getvalue())
