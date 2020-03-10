# s tatic version
from io import TextIOWrapper

from reportlab.graphics import renderPDF
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, KeepTogether

def addPageNumber(canvas:canvas.Canvas, doc):
    """
    Add the page number
    """
    page_num = canvas.getPageNumber()
    text = f"Page {page_num}"
    canvas.setFontSize(10)
    canvas.drawCentredString(doc.pagesize[0]/2, doc.bottomMargin / 2, text)

def csv2pdftable(fh, title: str, saveloc: str = None):
    """
    :param fh: str of the filepath or a TextIOWrapper in a 'r' mode
    """
    if not isinstance(fh, TextIOWrapper):
        fh = open(fh, 'r')

    styles = getSampleStyleSheet()
    styleN = styles['Normal']
    styleN.spaceAfter = 2
    print(styles)
    styleT = [
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
    ]
    doc = SimpleDocTemplate(f'{title}.pdf')
    flow = []
    lines = [line for line in fh.readlines()]
    space = Paragraph("<br />""<br />", styleN)

    title = ''
    for line in lines[:2]:
        title += line.replace(',', '') + "<br />"
    flow.append(Paragraph(title, styles['Heading1']))

    efficiency = lines[3].strip().split(',')[:2]
    flow.append(Table([efficiency], style=styleT,
                      hAlign='LEFT', spaceBefore=2*styleN.fontSize,spaceAfter=2*styleN.fontSize))

    volumes = []
    for line in lines[5:11]:
        volumes.append(line.strip().split(',')[:2])
    flow.append(Table(volumes, style=styleT, hAlign='LEFT', spaceAfter=styleN.fontSize))

    outlets = [Paragraph(lines[12].replace(',', ''), styleN)]
    table = []
    for line in lines[13:17]:
        table.append(line.strip().split(',')[:2])
    outlets.append(Table(table, style=styleT, hAlign='LEFT'))
    outlets.append(Paragraph(lines[17].replace(',', '')+ "<br />"+ "<br />", styleN))
    flow.append(KeepTogether(outlets))

    meters = []
    end = None
    date = None
    offset = 18
    #find idx for end of meter table
    for index, line in enumerate(lines[offset:]):
        if index == 0:
            p = line.strip().split(',')
        else:
            p = [Paragraph(l, styleN) for l in line.strip().split(',')]
        meters.append(p)
        if "Total," in line:
            end = index
            date = lines[index+2+offset]
            break
    styleT2 = [
        ('GRID', (0, 0), (-1, -2), 0.5, colors.black),
        ('GRID', (1, -1), (-1, -1), 0.5, colors.black)
    ]
    table = Table(meters, style=styleT2, hAlign='LEFT', spaceAfter=styleN.fontSize, repeatRows=1)
    table._colWidths[0] = 3*cm
    flow.append(table)

    flow.append(Paragraph(date.replace(',', '').strip(), styleN))

    meters_not_read = None
    table = []
    for index, line in enumerate(lines[end+2:]):
        if 'meters not read' in line:
            meters_not_read = [Paragraph(line.replace(',', ''), styleN)]
        elif meters_not_read is not None:
            table.append(line.strip().replace(',', ''))

    #reshaping the last table into three columns
    import numpy as np
    table = np.array(table)
    tries = 0
    while len(table) % 3 != 0:
        table = np.pad(table, pad_width=(0,1), mode='constant', constant_values=' ')
        tries += 1
        if tries == 3:
            print("Something went wrong with padding, aborting meters_not_read.")
            tries = None
            break

    if tries is not None and len(table) > 0:
        table = table.reshape([-1, 3])
        table = table.tolist()

        table = Table(table, hAlign='LEFT', spaceAfter=styleN.fontSize)
        meters_not_read.append(table)
        flow.append(KeepTogether(meters_not_read))

    doc.build(flow, onLaterPages=addPageNumber)

if __name__ == "__main__":
    title = "../out/WARBURN_SysEff-20191217-report"
    with open(f"{title}.csv", 'r') as fh:
        csv2pdftable(fh, title)