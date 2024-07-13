from flask import Flask
from flask import *
from flask_sqlalchemy import SQLAlchemy
import datetime
from errorHandler import InvalidUsage
import requests
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, lit
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType
import pycountry
import sys
from sqlalchemy import and_


app =Flask (__name__ )



app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:root@localhost/f1'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)





class Country(db.Model):
    __tablename__ = 'countries'
    country_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    country_code = db.Column(db.String(4))
    country_name = db.Column(db.String(255))

    circuits = db.relationship('Circuit', backref='country')

    def serialize(self):
        return {
            "country_id": self.country_id,
            "country_code": self.country_code,
            "country_name": self.country_name,
            "circuits": [circuit.serialize1() for circuit in self.circuits]
        }

class Circuit(db.Model):
    __tablename__ = 'curcuits'
    circuit_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    circuit_short_name = db.Column(db.String)
    country_id = db.Column(db.BigInteger, db.ForeignKey('countries.country_id'))
    id_ergast = db.Column(db.String())

    def serialize(self):
        return {
            "circuit_id": self.circuit_id,
            "circuit_short_name": self.circuit_short_name,

        }
    def serialize2(self):
        return {
            "circuit_id": self.circuit_id,
            "circuit_short_name": self.circuit_short_name,
            "country": self.country.serialize()

        }

class Meeting(db.Model):
    __tablename__ = 'meetings'
    meeting_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    meeting_name = db.Column(db.String(255))
    meeting_official_name = db.Column(db.String(500))
    circuit_id = db.Column(db.BigInteger, db.ForeignKey('curcuits.circuit_id'))
    date_start = db.Column(db.DateTime)
    gmt_offset = db.Column(db.Interval)
    season = db.Column(db.Integer)
    no = db.Column(db.Integer)
    circuit = db.relationship('Circuit', backref='meetings')

    sessions = db.relationship("Session", back_populates="meeting")

    def serialize(self):
        return {
            "meeting_id": self.meeting_id,
            "meeting_name": self.meeting_name,
            "meeting_official_name": self.meeting_official_name,
            "circuit": self.circuit.serialize(),
            "date_start": self.date_start.isoformat(),
            "gmt_offset": str(self.gmt_offset),
            "season": self.season,
            "no": self.no
        }
    

class Session(db.Model):
    __tablename__ = 'sessions'

    session_id = db.Column(db.Integer, primary_key=True)
    og_session_id = db.Column(db.Integer, nullable=True)
    session_type = db.Column(db.String, nullable=True)
    session_name = db.Column(db.String, nullable=True)
    meeting_id = db.Column(db.Integer, db.ForeignKey('meetings.meeting_id'), nullable=False)

    meeting = db.relationship("Meeting", back_populates="sessions")

    def __init__(self, session_id, og_session_id, session_type, session_name, meeting_id):
        self.session_id = session_id
        self.og_session_id = og_session_id
        self.session_type = session_type
        self.session_name = session_name
        self.meeting_id = meeting_id
    
    def serialize(self):
        return {
            "session_id": self.session_id,
            "og_session_id": self.og_session_id,
            "session_type": self.session_type,
            "session_name": self.session_name,
            "meeting": self.meeting.serialize()
        }


class Position(db.Model):
    __tablename__ = 'positions'
    
    position_id = db.Column(db.Integer, primary_key=True)
    driver_number = db.Column(db.Integer, nullable=False)
    position =db.Column(db.Integer, nullable=False)
    session_id = db.Column(db.Integer, db.ForeignKey('sessions.session_id'), nullable=False)

    def __init__(self, driver_number, position, session_id):
        self.driver_number = driver_number
        self.position = position
        self.session_id = session_id
    
    #session = db.relationship("Session", back_populates="positions")


'''spark = SparkSession.builder \
    .appName("YourAppName") \
    .getOrCreate()'''

API_BASE_URL = 'https://api.openf1.org/v1/'
API_BASE_URL_OLD = 'https://ergast.com/api/f1/'
API_BASE_NAME = 'OPEN F1'



#handling invalid usage of routes
@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

@app.route('/')
def hello_world():
    return'Hello, World!'


def create_country_if_not_exists(country_id, country_code, country_name):
    country = Country.query.filter_by(country_id=country_id).first()
    if not country:
        country = Country(country_id = country_id, country_code=country_code, country_name=country_name)
        db.session.add(country)
        db.session.commit()
        return country


def create_circuit_if_not_exists_name(circuit_short_name, country_id, cir_id, id_old):
    circuit = Circuit.query.filter_by(circuit_short_name=circuit_short_name).first()
    
    if not circuit:
        print("inserted "+circuit_short_name+"\n")
        new_circuit = Circuit(circuit_short_name=circuit_short_name, country_id=country_id, circuit_id=cir_id, id_ergast=id_old)
        db.session.add(new_circuit)
        db.session.commit()
    else:
        if(circuit.
           id_ergast==None):
            circuit.id_ergast = id_old
            db.session.commit()

def create_circuit_if_not_exists(cir_id, circuit_short_name, country_id):
    circuit = Circuit.query.filter_by(circuit_id=cir_id).first()
    if not circuit:
        
        new_circuit = Circuit(circuit_id = cir_id,circuit_short_name=circuit_short_name, country_id=country_id)
        db.session.add(new_circuit)
        db.session.commit()

def insert_meeting_if_not_exists(meeting_id, meeting_name, meeting_official_name, circuit_id, date_start, gmt_offset, season, no=None):
    meeting = Meeting.query.filter_by(meeting_id=meeting_id).first()
    if not meeting:
        new_meeting = Meeting(meeting_id=meeting_id,
                              meeting_name=meeting_name,
                              meeting_official_name=meeting_official_name,
                              circuit_id=circuit_id,
                              date_start=date_start,
                              gmt_offset=gmt_offset,
                              season=season,
                              no=no)
        db.session.add(new_meeting)
        db.session.commit()

@app.route('/meetings_for_year',methods=['GET'])
def dog_za_god():
    try:
        year = int(request.args['year'])
        if(year<=2013 or year >datetime.datetime.now().year):
            raise InvalidUsage('Invalid year', status_code=410)
        races = Meeting.query.filter_by(season=year).all()
        if races:
            return jsonify([race.serialize() for race in races])
        url = API_BASE_URL+'meetings?year='+str(year)
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            for d in data:
                insert_meeting_if_not_exists(d['meeting_key'], d['meeting_name'], d['meeting_official_name'], d['circuit_key'], d['date_start'], d['gmt_offset'], d['year'])
                create_circuit_if_not_exists(d['circuit_key'], d['circuit_short_name'], d['country_key'])
                create_country_if_not_exists(d['country_key'], d['country_code'], d['country_name'])
            return data
        else:
            raise InvalidUsage('Error while retrieving the data', status_code=410)
        
    except KeyError:
         raise InvalidUsage('The "year" parameter is missing.', status_code=410)
    

@app.route('/meetings_for_year_old',methods=['GET'])
def dog_za_god_old():
    try:
        year = int(request.args['year'])
        if(year<=2013 or year >datetime.datetime.now().year):
            raise InvalidUsage('Invalid year', status_code=410)
        races = Meeting.query.filter_by(season=year).all()
        if races:
            return jsonify([race.serialize() for race in races])
        url = API_BASE_URL_OLD+'meetings?year='+str(year)
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            for d in data:
                insert_meeting_if_not_exists(d['meeting_key'], d['meeting_name'], d['meeting_official_name'], d['circuit_key'], d['date_start'], d['gmt_offset'], d['year'])
                create_circuit_if_not_exists(d['circuit_key'], d['circuit_short_name'], d['country_key'])
                create_country_if_not_exists(d['country_key'], d['country_code'], d['country_name'])
            return data
        else:
            raise InvalidUsage('Error while retrieving the data', status_code=410)
        
    except KeyError:
         raise InvalidUsage('The "year" parameter is missing.', status_code=410)
    

@app.route('/circuits',methods=['GET'])
def add_circuits():
    try:
        
        url = API_BASE_URL_OLD+'circuits.json?limit=300'
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            i=50000
            j=100000
            circs = data['MRData']['CircuitTable']['Circuits']
            for cir in circs:
                
                print(cir)
                country_name = cir["Location"]["country"]
                
                country = Country.query.filter_by(country_name=country_name).first()
                if not country:
                    country = Country(country_id = i, country_code=None, country_name=country_name)
                    db.session.add(country)
                    db.session.commit()
                    i+=10

                create_circuit_if_not_exists_name(cir["circuitName"], country.country_id, j, cir["circuitId"])
                j+=10
        else:
            raise InvalidUsage('Error while retrieving the data', status_code=410)
        return "OK"
        
    except KeyError:
         raise InvalidUsage('The "year" parameter is missing.', status_code=410)
    

@app.route('/get_countries',methods=['GET'])
def get_countries():
    countries = Country.query.all()
    return jsonify([c.serialize() for c in countries])

def insert_session_if_not_exists(session_id, session_type, meeting_id, og_session_id=None, session_name=None):
    session = Session.query.filter_by(session_id=session_id).first()
    if not session:
        new_session = Session(session_id=session_id, og_session_id=og_session_id, session_type=session_type, session_name=session_name, meeting_id=meeting_id)
        db.session.add(new_session)
        db.session.commit()

@app.route('/add_sessions_new',methods=['GET'])
def add_sessions_new():
    try:
        year=2023
        sessionsInYear = Meeting.query.filter_by(season=year).all()
        for session in sessionsInYear:
            url = API_BASE_URL+'sessions?meeting_key='+str(session.meeting_id)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                for d in data:
                    insert_session_if_not_exists(d['session_key'], d['session_type'], session.meeting_id, None, d['session_name'])
            else:
                raise InvalidUsage('Error while retrieving the data', status_code=410)
        return "Success"
    except KeyError:
        raise InvalidUsage('The "year" parameter is missing.', status_code=410)
    

@app.route('/add_sessions_old',methods=['GET'])
def add_sessions_old():
    try:
        year=request.args['year']
        #sessionsInYear = Meeting.query.filter_by(season=year).all()
        url = API_BASE_URL_OLD+str(year)+'.json'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()['MRData']['RaceTable']['Races']
            for d in data:
                meeting = Meeting.query.filter_by(season=year, no=d['round']).first()
                insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round']+"4","Qualifying", meeting.meeting_id, None, "Qualifying")
                insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round'],"Race", meeting.meeting_id, None, "Race")
        
                if "FirstPractice" in d:
                    insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round']+"1","Practice", meeting.meeting_id, None, "Practice 1")
                if "SecondPractice" in d:
                    insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round']+"2","Practice", meeting.meeting_id, None, "Practice 2")
                if "ThirdPractice" in d:
                    insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round']+"3","Practice", meeting.meeting_id, None, "Practice 3")  
                if "Sprint" in d:
                    insert_session_if_not_exists(str(meeting.meeting_id)+""+d['round']+"5","Race", meeting.meeting_id, None, "Sprint")
                #print(d)
                #print("--------------------------------------------------")
        
        else:
            raise InvalidUsage('Error while retrieving the data', status_code=410)
        return "Success"
    except KeyError:
        raise InvalidUsage('The "year" parameter is missing.', status_code=410)

@app.route('/add_meetings_old',methods=['GET'])
def add_meetings_old():
    try:
        year = 2014
        url = API_BASE_URL_OLD+str(year)+'.json'
        print(url)
        response = requests.get(url)
    
        if response.status_code == 200:
            data = response.json()
            i=50000
            j=Meeting.query.order_by(Meeting.meeting_id.desc()).first().meeting_id+10
            races = data['MRData']['RaceTable']['Races']
            for d in races:
                
                '''print(cir)
                country_name = cir["Location"]["country"]
                
                country = Country.query.filter_by(country_name=country_name).first()
                if not country:
                    country = Country(country_id = i, country_code=None, country_name=country_name)
                    db.session.add(country)
                    db.session.commit()
                    i+=10

                create_circuit_if_not_exists_name(cir["circuitName"], country.country_id, j)'''
                #print(d['season']+" "+d['round']+" "+d['raceName'])
                circId = d['Circuit']['circuitId']
                cir = Circuit.query.filter_by(id_ergast=circId).first()
                #print(cir)
                circId = cir.circuit_id
                date = d['date']
                time = d['time'][:-1]
                datetime = date+" "+time
                insert_meeting_if_not_exists(j, d['raceName'], None, circId , datetime, "00:00:00", d['season'], d['round'])
                j+=10
        else:
            raise InvalidUsage('Error while retrieving the data', status_code=410)
        return jsonify([m.serialize() for m in Meeting.query.filter(
            Meeting.season == year
        ).order_by(Meeting.no.asc()).all()])
        
        
    except KeyError:
         raise InvalidUsage('The "year" parameter is missing.', status_code=410)


@app.route('/positions',methods=['GET'])
def get_positions():
    try:
        sessions = Session.query.filter(and_(Session.session_id<10000)).all()

        for session in sessions:
            url = API_BASE_URL+'position?session_key='+str(session.session_id)
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                final_pos = {}
                for d in data:
                    final_pos[d['driver_number']] = d['position']
                #return final_pos
                for key, value in final_pos.items():
                    pos = Position.query.filter_by(driver_number=key, session_id=session.session_id).first()
                    if pos:
                        continue
                    else:
                        try:
                            new_pos =  Position(driver_number=key, position=value, session_id=session.session_id)
                            db.session.add(new_pos)
                            db.session.commit()
                        except Exception as e:
                            print(e)
            
            else:
                raise InvalidUsage('Error while retrieving the data', status_code=410)
            print(session.session_name +" in "+str(session.session_id)+" done\n")
        return "Success"
    except KeyError:
        raise InvalidUsage('The "year" parameter is missing.', status_code=410)



@app.route('/positions_old',methods=['GET'])
def get_positions_old():
    sessions = Session.query.join(Meeting).filter(and_(Meeting.season==request.args['year'], Session.session_name=="Qualifying")).all()
    for s in sessions:
        url = API_BASE_URL_OLD+str(s.meeting.season)+'/'+str(s.meeting.no)+'/qualifying.json'
        response = requests.get(url)
        data = response.json()
        #print(data)
        data = data['MRData']['RaceTable']['Races']
        for d in data:
            for p in d['QualifyingResults']:
                print(p)
                posEx = Position.query.filter_by(driver_number=p['number'], session_id=s.session_id).first()
                if posEx:
                    continue
                else:
                    pos = Position(p['number'], p['position'], s.session_id)
                    db.session.add(pos)
                    db.session.commit()
    return "Success"


from sqlalchemy import text
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.exceptions import ConvergenceWarning
import warnings
from sklearn.ensemble import GradientBoostingRegressor

@app.route('/data',methods=['GET'])
def get_data():
    sql_query = text("""WITH pomocna AS (
	SELECT *, (
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	)-P.position as gain FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
	WHERE S.session_name='Race'
)
                     
                     SELECT position_id, driver_number, "position", meeting_name,season, "no",
	(CASE WHEN "no"=1 THEN 21
	ELSE
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no-1) AND session_name='Race' AND PP.driver_number = P.driver_number
	),21) END) as previous_race_pos,
	COALESCE((
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE (MM.season-1)=MMM.season AND session_name='Race' AND MM.meeting_name=MMM.meeting_name AND PP.driver_number = P.driver_number
	),21) AS previous_year_pos,
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	),21) AS qualy_pos,
                     COALESCE((SELECT ROUND(AVG(gain),0)
	FROM pomocna POM
	WHERE MM.season = POM.season AND (POM.no = (MM.no-1) OR POM.no = (MM.no-2) OR POM.no = (MM.no-3)) AND P.driver_number = POM.driver_number),0) as gain,
                     
    COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Sprint' AND PP.driver_number = P.driver_number
                     ),21) as sprint_pos
FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
WHERE session_name='Race'
ORDER BY season ASC, "no" ASC, "position" ASC
""")

    result = db.session.execute(sql_query)


    X = []
    y = []
    for r in result:
        X.append([r[6], r[7], r[8], int(r[10])])  # previous_race_pos, previous_year_pos, qualy_pos
        y.append(r[2])  # fin_pos

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logreg = LogisticRegression(max_iter=100000, solver='lbfgs')
    logreg.fit(X_train, y_train)

    dtc = DecisionTreeClassifier()
    dtc.fit(X_train, y_train)

    rfc = RandomForestClassifier()
    rfc.fit(X_train, y_train)

    logreg_pred = logreg.predict(X_test)
    dtc_pred = dtc.predict(X_test)
    rfc_pred = rfc.predict(X_test)

    logreg_accuracy = accuracy_score(y_test, logreg_pred)
    dtc_accuracy = accuracy_score(y_test, dtc_pred)
    rfc_accuracy = accuracy_score(y_test, rfc_pred)


    gbr = GradientBoostingRegressor()
    gbr.fit(X_train, y_train)

    gbr_pred = gbr.predict(X_test)

    gbr_pred_classes = [int(round(pred)) for pred in gbr_pred]

    gbr_accuracy = accuracy_score(y_test, gbr_pred_classes)

    print("Gradient Boosting Regressor Accuracy:", gbr_accuracy)
    print("Logistic Regression Accuracy:", logreg_accuracy)
    print("Decision Tree Classifier Accuracy:", dtc_accuracy)
    print("Random Forest Classifier Accuracy:", rfc_accuracy)

    from sklearn.svm import SVC

    svc = SVC()
    svc.fit(X_train, y_train)


    svc_pred = svc.predict(X_test)

    svc_accuracy = accuracy_score(y_test, svc_pred)

    print("Support Vector Classifier Accuracy:", svc_accuracy)
    return ""


@app.route('/data_class',methods=['GET'])
def get_data_class():
    sql_query = text("""
                     WITH pomocna AS (
	SELECT *, (
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	)-P.position as gain FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
	WHERE S.session_name='Race'
)
                     
                     SELECT position_id, driver_number, CASE WHEN ("position"<=10 AND "position">=1)  THEN 'IN' ELSE 'OUT' END as fin_pos, meeting_name,season, "no",
	(CASE WHEN "no"=1 THEN 21
	ELSE
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no-1) AND session_name='Race' AND PP.driver_number = P.driver_number
	),21) END) as previous_race_pos,
	COALESCE((
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE (MM.season-1)=MMM.season AND session_name='Race' AND MM.meeting_name=MMM.meeting_name AND PP.driver_number = P.driver_number
	),21) AS previous_year_pos,
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	),21) AS qualy_pos,COALESCE((SELECT ROUND(AVG(gain),0)
    
	FROM pomocna POM
	WHERE MM.season = POM.season AND (POM.no = (MM.no-1) OR POM.no = (MM.no-2) OR POM.no = (MM.no-3)) AND P.driver_number = POM.driver_number),0) as gain,
                     COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Sprint' AND PP.driver_number = P.driver_number
                     ),21) as sprint_pos
FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
WHERE session_name='Race'
ORDER BY season ASC, "no" ASC, "position" ASC
""")
    
    import pandas as pd

    result = db.session.execute(sql_query)



    rows = result.fetchall()

    columns = ['position_id', 'driver_number', 'fin_pos', 'meeting_name', 'season', 'no', 'previous_race_pos', 'previous_year_pos', 'qualy_pos','gain','sprint_pos']
    df = pd.DataFrame(rows, columns=columns)


    #print(df.head())


    df['fin_pos'] = df['fin_pos'].apply(lambda x: 1 if x == 'IN' else 0)


    X = df[['previous_race_pos', 'previous_year_pos', 'qualy_pos','sprint_pos']]
    y = df['fin_pos']

    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, accuracy_score

    accuracy_scores_lr = []
    classification_reports_lr = []
    accuracy_scores_rf = []
    classification_reports_rf = []
    accuracy_scores_svm = []
    classification_reports_svm = []
    accuracy_scores_gb = []
    classification_reports_gb = []

    tscv = TimeSeriesSplit(n_splits=5)  


    for train_index, test_index in tscv.split(X):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        

        model_lr = LogisticRegression(max_iter=1000)
        model_lr.fit(X_train, y_train)
        

        y_pred_lr = model_lr.predict(X_test)
        


        accuracy_lr = accuracy_score(y_test, y_pred_lr)
        classification_report_lr = classification_report(y_test, y_pred_lr, output_dict=True)
        

        accuracy_scores_lr.append(accuracy_lr)
        classification_reports_lr.append(classification_report_lr)
       
        from sklearn.ensemble import RandomForestClassifier

    

        model_rf = RandomForestClassifier(random_state=48)
        model_rf.fit(X_train, y_train)
        

        y_pred_rf = model_rf.predict(X_test)

        accuracy_rf = accuracy_score(y_test, y_pred_rf)
        classification_report_rf = classification_report(y_test, y_pred_rf, output_dict=True)
        

        accuracy_scores_rf.append(accuracy_rf)
        classification_reports_rf.append(classification_report_rf)




        from sklearn.svm import SVC
        from sklearn.preprocessing import StandardScaler

        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        

        model_svm = SVC(kernel='linear', C=1.2, random_state=48)
        model_svm.fit(X_train_scaled, y_train)
        

        y_pred_svm = model_svm.predict(X_test_scaled)

        accuracy_svm = accuracy_score(y_test, y_pred_svm)
        classification_report_svm = classification_report(y_test, y_pred_svm, output_dict=True)
        

        accuracy_scores_svm.append(accuracy_svm)
        classification_reports_svm.append(classification_report_svm)
        
        


        from sklearn.ensemble import GradientBoostingClassifier
        model_gb = GradientBoostingClassifier(random_state=48)
        model_gb.fit(X_train, y_train)
        

        y_pred_gb = model_gb.predict(X_test)

        accuracy_gb = accuracy_score(y_test, y_pred_gb)
        classification_report_gb = classification_report(y_test, y_pred_gb, output_dict=True)
        

        accuracy_scores_gb.append(accuracy_gb)
        classification_reports_gb.append(classification_report_gb)
            
        
    
    import numpy as np

    avg_accuracy_lr = np.mean(accuracy_scores_lr)
    avg_classification_report_lr = classification_report(y_test, y_pred_lr)

    print("Logistic Regression - Cumulative Results:")
    print("Average Accuracy:", avg_accuracy_lr)
    print("Average Classification Report:")
    print(avg_classification_report_lr)


    avg_accuracy_rf = np.mean(accuracy_scores_rf)
    avg_classification_report_rf = classification_report(y_test, y_pred_rf)

    print("Random Forest Classifier - Cumulative Results:")
    print("Average Accuracy:", avg_accuracy_rf)
    print("Average Classification Report:")
    print(avg_classification_report_rf)

    avg_accuracy_svm = np.mean(accuracy_scores_svm)
    avg_classification_report_svm = classification_report(y_test, y_pred_svm)

    print("Support Vector Machine (SVM) - Cumulative Results:")
    print("Average Accuracy:", avg_accuracy_svm)
    print("Average Classification Report:")
    print(avg_classification_report_svm)

    avg_accuracy_gb = np.mean(accuracy_scores_gb)
    avg_classification_report_gb = classification_report(y_test, y_pred_gb)

    print("Gradient Boosting Classifier - Cumulative Results:")
    print("Average Accuracy:", avg_accuracy_gb)
    print("Average Classification Report:")
    print(avg_classification_report_gb)
    return ""

@app.route('/data_podium',methods=['GET'])
def get_data_podium():
    sql_query = text("""WITH pomocna AS (
	SELECT *, (
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	)-P.position as gain FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
	WHERE S.session_name='Race'
)
                     
                     SELECT position_id, driver_number, CASE WHEN ("position"<=3 AND "position">=1) THEN 'IN' ELSE 'OUT' END as fin_pos, meeting_name,season, "no",
	(CASE WHEN "no"=1 THEN 21
	ELSE
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no-1) AND session_name='Race' AND PP.driver_number = P.driver_number
	),21) END) as previous_race_pos,
	COALESCE((
	SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE (MM.season-1)=MMM.season AND session_name='Race' AND MM.meeting_name=MMM.meeting_name AND PP.driver_number = P.driver_number
	),21) AS previous_year_pos,
	COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Qualifying' AND PP.driver_number = P.driver_number
	),21) AS qualy_pos,
                     COALESCE((SELECT ROUND(AVG(gain),0)
	FROM pomocna POM
	WHERE MM.season = POM.season AND (POM.no = (MM.no-1) OR POM.no = (MM.no-2) OR POM.no = (MM.no-3)) AND P.driver_number = POM.driver_number),0) as gain,
                     COALESCE((SELECT position
	FROM positions PP JOIN sessions SS USING (session_id) JOIN meetings MMM USING (meeting_id)
	WHERE MM.season=MMM.season AND MMM.no = (MM.no) AND session_name='Sprint' AND PP.driver_number = P.driver_number
                     ),-1) as sprint_pos
FROM positions P JOIN sessions S USING (session_id) JOIN meetings MM USING (meeting_id)
WHERE session_name='Race'
ORDER BY season ASC, "no" ASC, "position" ASC
""")
    
    import pandas as pd

    result = db.session.execute(sql_query)



    rows = result.fetchall()


    columns = ['position_id', 'driver_number', 'fin_pos', 'meeting_name', 'season', 'no', 'previous_race_pos', 'previous_year_pos', 'qualy_pos','gain','sprint_pos']
    df = pd.DataFrame(rows, columns=columns)

    #print(df.head())


    df['fin_pos'] = df['fin_pos'].apply(lambda x: 1 if x == 'IN' else 0)


    XX = []
    XX.append(df[['previous_race_pos', 'previous_year_pos', 'qualy_pos']])
    XX.append(df[['previous_race_pos', 'previous_year_pos', 'qualy_pos','gain']])
    XX.append(df[['previous_race_pos', 'previous_year_pos', 'qualy_pos','sprint_pos']])
    XX.append(df[['previous_race_pos', 'previous_year_pos', 'qualy_pos','gain','sprint_pos']])
    y = df['fin_pos']

    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, accuracy_score

    for i in range(0, 4):
        print(i)
        X = XX[i]
        y = df['fin_pos']
        accuracy_scores_lr = []
        classification_reports_lr = []
        accuracy_scores_rf = []
        classification_reports_rf = []
        accuracy_scores_svm = []
        classification_reports_svm = []
        accuracy_scores_gb = []
        classification_reports_gb = []

        tscv = TimeSeriesSplit(n_splits=20) 


        for train_index, test_index in tscv.split(X):
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            y_train, y_test = y.iloc[train_index], y.iloc[test_index]
            

            model_lr = LogisticRegression(max_iter=1000)
            model_lr.fit(X_train, y_train)
            

            y_pred_lr = model_lr.predict(X_test)

            accuracy_lr = accuracy_score(y_test, y_pred_lr)
            classification_report_lr = classification_report(y_test, y_pred_lr, output_dict=True)
            

            accuracy_scores_lr.append(accuracy_lr)
            classification_reports_lr.append(classification_report_lr)
        
            from sklearn.ensemble import RandomForestClassifier

        

            model_rf = RandomForestClassifier(random_state=48)
            model_rf.fit(X_train, y_train)
            

            y_pred_rf = model_rf.predict(X_test)

            accuracy_rf = accuracy_score(y_test, y_pred_rf)
            classification_report_rf = classification_report(y_test, y_pred_rf, output_dict=True)
            

            accuracy_scores_rf.append(accuracy_rf)
            classification_reports_rf.append(classification_report_rf)




            from sklearn.svm import SVC
            from sklearn.preprocessing import StandardScaler

            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            

            model_svm = SVC(kernel='linear', C=1.0, random_state=42)
            model_svm.fit(X_train_scaled, y_train)
            

            y_pred_svm = model_svm.predict(X_test_scaled)

            accuracy_svm = accuracy_score(y_test, y_pred_svm)
            classification_report_svm = classification_report(y_test, y_pred_svm, output_dict=True)
            

            accuracy_scores_svm.append(accuracy_svm)
            classification_reports_svm.append(classification_report_svm)
            
            


            from sklearn.ensemble import GradientBoostingClassifier
            model_gb = GradientBoostingClassifier(random_state=42)
            model_gb.fit(X_train, y_train)
            
   
            y_pred_gb = model_gb.predict(X_test)
           
            accuracy_gb = accuracy_score(y_test, y_pred_gb)
            classification_report_gb = classification_report(y_test, y_pred_gb, output_dict=True)
            
   
            accuracy_scores_gb.append(accuracy_gb)
            classification_reports_gb.append(classification_report_gb)
                
            
        
        import numpy as np
        from sklearn.svm import SVC

        avg_accuracy_lr = np.mean(accuracy_scores_lr)
        avg_classification_report_lr = classification_report(y_test, y_pred_lr)

        print("Logistic Regression - Cumulative Results:")
        print("Average Accuracy:", avg_accuracy_lr)
        print("Average Classification Report:")
        print(avg_classification_report_lr)


        avg_accuracy_rf = np.mean(accuracy_scores_rf)
        avg_classification_report_rf = classification_report(y_test, y_pred_rf)

        print("Random Forest Classifier - Cumulative Results:")
        print("Average Accuracy:", avg_accuracy_rf)
        print("Average Classification Report:")
        print(avg_classification_report_rf)

        avg_accuracy_svm = np.mean(accuracy_scores_svm)
        avg_classification_report_svm = classification_report(y_test, y_pred_svm)

        print("Support Vector Machine (SVM) - Cumulative Results:")
        print("Average Accuracy:", avg_accuracy_svm)
        print("Average Classification Report:")
        print(avg_classification_report_svm)

        avg_accuracy_gb = np.mean(accuracy_scores_gb)
        avg_classification_report_gb = classification_report(y_test, y_pred_gb)

        print("Gradient Boosting Classifier - Cumulative Results:")
        print("Average Accuracy:", avg_accuracy_gb)
        print("Average Classification Report:")
        print(avg_classification_report_gb)
        print("--------------------------------------------------------------------------------------------\n")
    return ""



if __name__ =='__main__':
    app.run(debug=True)