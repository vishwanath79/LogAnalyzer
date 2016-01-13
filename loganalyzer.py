# Sample Flask app that greps logs and displays them in a graph. Tested out bokeh and pygal.

import datetime
import glob
import os
import subprocess
import time
from datetime import timedelta
from os import listdir
from os.path import isfile, join

import pandas as pd
import pygal
from Logconfig import *
from bokeh._legacy_charts import Bar
from bokeh.embed import components
from bokeh.models import HoverTool
from bokeh.resources import INLINE
from bokeh.util.string import encode_utf8
from flask import Flask
from flask import render_template

today = datetime.datetime.now() - timedelta(hours=7)
yesterday = today - datetime.timedelta(days=2)
app = Flask(__name__)


# List all files in directory. Unused.
@app.route("/list")
def list():
    onlyfiles = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    countfiles = len([name for name in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, name))])
    return render_template("emr.html", files=onlyfiles, count=countfiles)


# Home - All Log EMR clusters
@app.route("/")
@app.route("/queries")
def check_queries():
    onlyfiles = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    countfiles = len(glob.glob1(filepath, "*.log"))
    countqueries = subprocess.check_output('cat ' + countpath + 'Log* | grep CMD | wc -l', shell=True)
    message = 'The number of queries to be run ' + str(countqueries)
    message2 = 'The number of queries run ' + str(countfiles)
    path = filepath + '/*.log'
    files = glob.glob(path)
    allfiles = []
    alllines = []
    for file in files:
        allfiles.append(file)
        lines = subprocess.check_output(['tail', '-8', file])
        alllines.append(lines)

    return render_template("emr.html", files=allfiles, count=message, message2=message2)


# All Log query details
@app.route("/querydetails")
def check_querydetails():
    onlyfiles = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    countfiles = len([name for name in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, name))])
    message = 'The number of queries run ' + str(countfiles)
    path = filepath + '/*.log'
    files = glob.glob(path)
    allfilesdet = []
    alllines = []
    for file in files:
        allfilesdet.append(file)
        try:
            lines = subprocess.check_output('tail -8 ' + file + ' \n', shell=True)
            alllines.append(lines)
        except:
            pass
    text = 'Query Details and Duration '
    return render_template("emr.html", lines=alllines, text=text, filesdet=allfilesdet)


# All Log Query Errors
@app.route("/errors")
def check_errors():
    countfiles = len([name for name in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, name))])
    path = filepath + '/*.log'
    files = glob.glob(path)
    errors = []
    for file in files:
        try:
            errlines = subprocess.check_output('tail -40 ' + file + ' | grep -B 3 ERROR', shell=True)
            errors.append(errlines)
        except:
            pass
    error_messsage = 'Errors: '
    return render_template("emr.html", errs=errors, error_messsage=error_messsage)


# Log panda dataframe
@app.route("/graph")
def graph():
    try:
        rundate = str(subprocess.check_output(
                'ls ' + filepath + '| grep * | grep -o "[0-9]\+" | cut -c 1-8',
                shell=True))
    except:
        rundate = str(time.strftime("%Y%m%d"))
    onlyfiles = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    countfiles = len([name for name in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, name))])
    message = 'The number of queries run ' + str(countfiles)
    path = filepath + '/*.log'
    files = glob.glob(path)
    allduration = []
    allstatus = []
    allstart = []
    allend = []
    onlyfilenamelist = []
    for file in files:
        head, onlyfilename = os.path.split(file)

        onlyfilenamelist.append(onlyfilename)
        try:
            # Check logs for fields like Query Complete, Duration, Status etc
            complete = subprocess.check_output('tail -8 ' + file + ' | grep -o "Query complete:.*" | cut -f 2 -d ":"  ',
                                               shell=True)
            duration = subprocess.check_output('tail -8 ' + file + ' | grep -o "Duration.*" | cut -f 2 -d ":" ',
                                               shell=True)

            status = subprocess.check_output('tail -8 ' + file + ' | grep -o "Status.*" | cut -f 2 -d ":" ',
                                             shell=True)
            starttime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e Start' + '| awk -F "Start Time:"' + " '{print $2}' ", shell=True)
            endtime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e End' + '| awk -F "End Time:"' + " '{print $2}' ", shell=True)
            allduration.append(duration)
            allstatus.append(status)
            allstart.append(starttime)
            allend.append(endtime)
        except:
            pass

    cleanupalllines = map(lambda s: s.strip(), onlyfilenamelist)
    cleanupduration = map(lambda s: s.strip(), allduration)
    cleanupstatus = map(lambda s: s.strip(), allstatus)
    cleanupstart = map(lambda s: s.strip(), allstart)
    cleanupend = map(lambda s: s.strip(), allend)
    # If you want actual sum of minutes
    sumd = 0
    floatallduration = []
    for item in cleanupduration:
        try:
            floatallduration.append(float(item))
            sumd += float(item)
        except:
            pass

    zipper = zip(cleanupalllines, floatallduration, cleanupstatus, cleanupstart, cleanupend)
    df = pd.DataFrame(zipper)
    # Create Dataframe based on words queried in the logs
    df = df.rename(columns={0: 'QUERY', 1: 'DURATION(MINS)', 2: 'STATUS', 3: 'START TIME', 4: 'END TIME'})
    df['Col5'] = rundate
    df = df.rename(columns={'Col5': 'Run Date'})
    df = df.sort_values(by='START TIME', ascending=True)
    filenamedate = 'Log' + "_" + str(rundate)
    df.to_csv(filenamedate, sep='\t')
    savemessage = 'File ' + filenamedate + ' saved '
    dfname = 'QUERY TIME SUMMARY'
    dfblankmessage = '*Blank fields indicate query is still running'
    return render_template("analysis.html", dfname=dfname, df=df, savemessage=savemessage,
                           dfblankmessage=dfblankmessage)


# Pygal Graph

@app.route("/trend.svg")
def pygalplot():
    try:
        rundate = str(subprocess.check_output(
                'ls ' + filepath + '| grep * | grep -o "[0-9]\+" | cut -c 1-8',
                shell=True))
    except:
        rundate = str(time.strftime("%Y%m%d"))
    onlyfiles = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    countfiles = len([name for name in os.listdir(filepath) if os.path.isfile(os.path.join(filepath, name))])
    message = 'The number of queries run ' + str(countfiles)
    path = filepath + '/*.log'
    files = glob.glob(path)
    allduration = []
    allstatus = []
    allstart = []
    allend = []
    onlyfilenamelist = []
    for file in files:
        head, onlyfilename = os.path.split(file)

        onlyfilenamelist.append(onlyfilename)
        try:
            complete = subprocess.check_output('tail -8 ' + file + ' | grep -o "Query complete:.*" | cut -f 2 -d ":"  ',
                                               shell=True)
            duration = subprocess.check_output('tail -8 ' + file + ' | grep -o "Duration.*" | cut -f 2 -d ":" ',
                                               shell=True)

            status = subprocess.check_output('tail -8 ' + file + ' | grep -o "Status.*" | cut -f 2 -d ":" ',
                                             shell=True)
            starttime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e Start' + '| awk -F "Start Time:"' + " '{print $2}' ", shell=True)
            endtime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e End' + '| awk -F "End Time:"' + " '{print $2}' ", shell=True)
            allduration.append(duration)
            allstatus.append(status)
            allstart.append(starttime)
            allend.append(endtime)
        except:
            pass

    cleanupalllines = map(lambda s: s.strip(), onlyfilenamelist)
    cleanupduration = map(lambda s: s.strip(), allduration)
    # If you want actual sum of minutes
    sumd = 0
    floatallduration = []
    for item in cleanupduration:
        try:
            floatallduration.append(float(item))
            sumd += float(item)
        except:
            pass
    results = [(int(x) if x else 0) for x in cleanupduration]
    print results
    line_chart = pygal.HorizontalBar(print_labels=True, print_values=True, height=2000)
    line_chart.title = 'Log Run Trend'
    line_chart.x_labels = cleanupalllines
    line_chart.add('Duration', results)
    return line_chart.render_response()


@app.route('/trend')
@app.route('/pygal.html')
def linechart():
    return render_template('pygal.html')


# Bokeh Graph
@app.route('/analyze')
def bokehplot():
    try:
        rundate = str(subprocess.check_output(
                'ls ' + logfilepath + '| grep * | grep -o "[0-9]\+" | cut -c 1-8',
                shell=True))
    except:
        rundate = str(time.strftime("%Y%m%d"))
    path = logfilepath + '/*.log'
    files = glob.glob(path)
    allduration = []
    allstatus = []
    allstart = []
    allend = []
    onlyfilenamelist = []
    for file in files:
        head, onlyfilename = os.path.split(file)
        onlyfilename = onlyfilename[:-13]
        onlyfilenamelist.append(onlyfilename)

        try:
            # dont think we need complete
            complete = subprocess.check_output('tail -8 ' + file + ' | grep -o "Query complete:.*" | cut -f 2 -d ":"  ',
                                               shell=True)

            duration = subprocess.check_output('tail -8 ' + file + ' | grep -o "Duration.*" | cut -f 2 -d ":" ',
                                               shell=True)

            status = subprocess.check_output('tail -8 ' + file + ' | grep -o "Status.*" | cut -f 2 -d ":" ',
                                             shell=True)

            starttime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e Start' + '| awk -F "Start Time:"' + " '{print $2}' ", shell=True)
            endtime = subprocess.check_output(
                    'tail -8 ' + file + '| grep -e End' + '| awk -F "End Time:"' + " '{print $2}' ", shell=True)
            allduration.append(duration)
            allstatus.append(status)
            allstart.append(starttime)
            allend.append(endtime)
        except:
            pass
    cleanupalllines = map(lambda s: s.strip(), onlyfilenamelist)
    cleanupduration = map(lambda s: s.strip(), allduration)
    cleanupstatus = map(lambda s: s.strip(), allstatus)
    cleanupstart = map(lambda s: s.strip(), allstart)
    cleanupend = map(lambda s: s.strip(), allend)
    sumd = 0
    floatallduration = []
    for item in cleanupduration:
        try:
            floatallduration.append(float(item))
            sumd += float(item)
        except:
            pass
    zipper = zip(cleanupalllines, floatallduration, cleanupstatus, cleanupstart, cleanupend)
    df = pd.DataFrame(zipper)
    df = df.rename(columns={0: 'QUERY', 1: 'DURATION(MINS)', 2: 'STATUS', 3: 'START TIME', 4: 'END TIME'})
    df['Col5'] = df['QUERY'].map(lambda x: str(x)[-8:])
    df['QUERY'] = df['QUERY'].map(lambda x: str(x)[:-13])
    df = df.rename(columns={'Col5': 'RunDate'})
    df = df.sort_values(by='QUERY', ascending=True)
    df = df[df['STATUS'].isin(["SUCCESS"])]
    pivot = pd.pivot_table(df.reset_index(), index='QUERY', columns='RunDate', values='DURATION(MINS)')
    pivot = pivot.fillna(0)
    TOOLS = 'crosshair,pan,wheel_zoom,box_zoom,reset,hover,previewsave'
    p = Bar(pivot, xlabel='QUERY', stacked=True, title="Log Query Comparison", legend='top_right', width=1100,
            height=800, tools=TOOLS)
    hover = p.select(dict(type=HoverTool))
    script, div = components(p, INLINE)
    # output_file("bar.html")
    # show(p)
    html = render_template("bokeh.html", script=script, div=div)
    return encode_utf8(html)


@app.errorhandler(403)
def forbidden(e):
    return render_template('error.html'), 403


@app.errorhandler(500)
def serverError(e):
    return render_template('error.html'), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8001))
    app.run(host=server, port=port, debug=True, threaded=True)
