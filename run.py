from flask import Flask, render_template, request, redirect, url_for
from flask_mysqldb import MySQL
import requests
import re
import pandas as pd
from os import walk, environ

app = Flask(__name__)

app.config['MYSQL_HOST'] = environ.get('MYSQL_HOST')
app.config['MYSQL_USER'] = environ.get('MYSQL_USER')
app.config['MYSQL_PASSWORD'] = environ.get('MYSQL_PASSWORD')
app.config['MYSQL_DB'] = environ.get('MYSQL_DB')

mysql = MySQL(app)

@app.route('/')
def index():
    return render_template("index.html")

### only for q1
@app.route('/trip', methods=['GET','POST'])
def trip():
    if request.method=='POST':
        date = request.form['date']
        return redirect(url_for('date' , date=date))
    return render_template('form.html')

@app.route('/date/<date>')
def date(date):
    cur = mysql.connection.cursor()
    query = "select hour, count(*) as trip_count from (select * from (select date(tpep_pickup_datetime) as date, hour(tpep_pickup_datetime) as hour from yellow_trip_jan) a where date = %s ) b group by hour order by hour asc"
    result = cur.execute(query, [(date,)])
    if result >0:
        userdetails= cur.fetchall()
        return render_template("countdb.html", userdetails = userdetails, date=date)

### main
@app.route('/choose', methods= ['GET','POST'])
def choose():
    if request.method=='POST':
        decision = request.form['dec']
        if decision == "Trip Counts":
            return redirect(url_for('count'))
        else:
            return redirect(url_for('price'))
    return render_template('choose.html')   

### trip count
@app.route('/count', methods=['GET', 'POST'])
def count():
    if request.method=='POST':
        date = request.form['date']
        return redirect(url_for('countdb', date = date))
    return render_template('count.html')

@app.route('/countdb/<date>')
def countdb(date):
    date = date
    fdate = date[0:7]
    durl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet".format(fdate)
    req = requests.get(durl)
    filename = req.url[durl.rfind('/')+1:]
    with open(filename, 'wb') as f:
        for chunk in req.iter_content(chunk_size=8192):
            f.write(chunk)
    root = app.root_path
    root = "{}/dataset/{}".format(root, filename)
    df = pd.read_parquet(root)
    df = df.fillna("0")
    cur = mysql.connection.cursor()
    res = cur.execute("show tables")
    if res>0:
        res = [item[0] for item in cur.fetchall()]
    tname = re.sub(r'[^\w]', '', filename)
    tname = tname[0:-7]
    if tname not in res:
        count_t = """select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'nyc_taxi' 
                    and table_name != 'yellow_trip_jan' """
        cur.execute(count_t)
        count_t_r = cur.fetchone()[0]
        if count_t_r > 2:
            tquery = """select table_name from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'nyc_taxi' 
                    and table_name != 'yellow_trip_jan' order by create_time asc limit 1"""
            cur.execute(tquery)
            tquery_r = cur.fetchone()[0]
            dquery = f"DROP TABLE {tquery_r} "
            cur.execute(dquery)
            mysql.connection.commit()
            cquery = f"""CREATE TABLE if not exists {tname} (
                        VendorID INT,
                        tpep_pickup_datetime DATETIME,
                        tpep_dropoff_datetime DATETIME,
                        passenger_count  INT,
                        trip_distance DOUBLE,
                        RatecodeID INT,
                        store_and_fwd_flag TEXT,
                        PULocationID INT,
                        DOLocationID INT,
                        payment_type INT,
                        fare_amount DOUBLE,
                        extra DOUBLE,
                        mta_tax DOUBLE,
                        tip_amount DOUBLE,
                        tolls_amount INT,
                        improvement_surcharge DOUBLE,
                        total_amount DOUBLE,
                        congestion_surcharge DOUBLE,
                        airport_fee DOUBLE )"""
            cur.execute(cquery)
            for i in df.itertuples(index=False, name=None):
                query = f"INSERT INTO {tname} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cur.execute(query, i)
                mysql.connection.commit()
    hquery = f"select hour, count(*) as trip_count from (select * from (select date(tpep_pickup_datetime) as date, hour(tpep_pickup_datetime) as hour from {tname}) a where date = %s ) b group by hour order by hour asc"
    result = cur.execute(hquery,[(date,)])
    if result >0:
        userdetails= cur.fetchall()
        return render_template("countdb.html", userdetails = userdetails, date=date)

#take in a location pair (start and end) and provide the cheapest
#hour of the day on average to take a trip.

@app.route('/price', methods=['GET', 'POST'])
def price():
    if request.method=='POST':
        para = request.form['month']
        para += " "
        para += request.form['pickup']
        para += " "
        para += request.form['dropoff']
        return redirect(url_for('pricedb', para = para))
    return render_template('price.html')

@app.route('/pricedb/<para>')
def pricedb(para):
    para = list(para.split(" "))
    month = para[0]
    pickup = int(para[1])
    dropoff = int(para[2])
    # find filename in local file
    # theres a standard naming convention 
    # if it exist dont download, else download (cloud query expensive, cheap to store in warehouse)
    # can jus download all files into the warehouse, minimise time to download from web
    local = next(walk("/Users/zixiang/Documents/test"), (None, None, []))[2]
    fn = f"yellow_tripdata_{month}.parquet"
    if fn in local:
        root = app.root_path
        root = "{}/dataset/{}".format(root, fn)
        tname = re.sub(r'[^\w]', '', fn)
        tname = tname[0:-7]
    else:
        durl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet".format(month)
        req = requests.get(durl)
        filename = req.url[durl.rfind('/')+1:]
        with open(filename, 'wb') as f:
            for chunk in req.iter_content(chunk_size=8192):
                f.write(chunk)
        root = app.root_path
        root = "{}/dataset/{}".format(root, filename)
        tname = re.sub(r'[^\w]', '', filename)
        tname = tname[0:-7]
    df = pd.read_parquet(root)
    df = df.fillna("0")
    cur = mysql.connection.cursor()
    res = cur.execute("show tables")
    if res>0:
        res = [item[0] for item in cur.fetchall()]
    if tname not in res:
        count_t = """select count(*) from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'nyc_taxi' 
                    and table_name != 'yellow_trip_jan' """
        cur.execute(count_t)
        count_t_r = cur.fetchone()[0]
        if count_t_r > 2:
            tquery = """select table_name from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = 'nyc_taxi' 
                    and table_name != 'yellow_trip_jan' order by create_time asc limit 1"""
            cur.execute(tquery)
            tquery_r = cur.fetchone()[0]
            dquery = f"DROP TABLE {tquery_r} "
            cur.execute(dquery)
            mysql.connection.commit()
            cquery = f"""CREATE TABLE if not exists {tname} (
                        VendorID INT,
                        tpep_pickup_datetime DATETIME,
                        tpep_dropoff_datetime DATETIME,
                        passenger_count  INT,
                        trip_distance DOUBLE,
                        RatecodeID INT,
                        store_and_fwd_flag TEXT,
                        PULocationID INT,
                        DOLocationID INT,
                        payment_type INT,
                        fare_amount DOUBLE,
                        extra DOUBLE,
                        mta_tax DOUBLE,
                        tip_amount DOUBLE,
                        tolls_amount INT,
                        improvement_surcharge DOUBLE,
                        total_amount DOUBLE,
                        congestion_surcharge DOUBLE,
                        airport_fee DOUBLE )"""
            cur.execute(cquery)
            for i in df.itertuples(index=False, name=None):
                query = f"INSERT INTO {tname} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cur.execute(query, i)
                mysql.connection.commit()
    hquery = f"""select hour(tpep_pickup_datetime), round(avg(total_amount - tip_amount),2) as fare from (
            select * from {tname} where PULocationID = '%s' and DOLocationID = '%s') a
            group by hour(tpep_pickup_datetime)
            order by fare asc
            limit 1"""     
    result = cur.execute(hquery, (pickup, dropoff))
    if result >0:
        userdetails= cur.fetchall()
        return render_template("pricedb.html", userdetails = userdetails, month=month, pickup=pickup, dropoff=dropoff)
    else:
        est_pickup = pickup // 3
        est_dropoff = dropoff // 3
        est_query = f"""select hour(tpep_pickup_datetime), avg(total_amount - tip_amount) as fare from 
                    (select * from 
                    (select *, truncate(PULocationID / 3, 0) as pu, truncate(DOLocationID /3,0) as dof from {tname}) a
                    where pu = '%s' and dof = '%s') b
                    group by hour(tpep_pickup_datetime)
                    order by fare asc
                    limit 1"""
        est_result = cur.execute(est_query, (est_pickup, est_dropoff))
        if est_result >0:
            det =  cur.fetchall()
            return render_template("priceest.html", det = det, month=month, pickup=pickup, dropoff=dropoff)
        else:
            return redirect(url_for('pricetry'))

@app.route('/pricetry', methods=['GET', 'POST'])
def pricetry():
    if request.method=='POST':
        para = request.form['month']
        para += " "
        para += request.form['pickup']
        para += " "
        para += request.form['dropoff']
        return redirect(url_for('pricedb', para = para))
    return render_template('pricetry.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template("404.html"), 404

@app.errorhandler(500)
def page_not_found(e):
    return render_template("500.html"), 500

if __name__ == '__main__':
    app.run(debug=True)