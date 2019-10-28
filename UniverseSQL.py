# Import the teradataml package for Vantage access
from teradataml import *
import pandas
import sys
from sqlalchemy.sql.expression import select, case as case_when, func
from sqlalchemy import TypeDecorator, Integer, String

def CreateTable(dbname, tablename, rowcount, freq):
    # Establish connection
    Vantage = 'tdap1627t2.labs.teradata.com'
    User = 'alice'
    Pass = 'alice'
    print('Connecting')
    sys.stderr.write('Connecting\n')
    con = create_context(Vantage, User, Pass)

    # create Table
    sys.stderr.write('Creating: '+dbname+'.'+tablename+'\n')
    ct_sql = """
        CREATE TABLE """+dbname+"""."""+tablename+""" (
           pkey BIGINT GENERATED ALWAYS AS IDENTITY
                   (START WITH 1 INCREMENT BY 1 MINVALUE -2147483647 MAXVALUE 2147483647 NO CYCLE)
          ,v1 VARCHAR(256)
          ,v2 float
    )"""
    #sys.stderr.write('Command: '+ct_sql+'\n')
    try:
        # drop if exists, no error if missing
        con.execute("drop table "+dbname+'.'+tablename)
    except:
        pass
    try:
        con.execute(ct_sql)
    except:
        sys.stderr.write('creating table failed'+'\n')
        return None

    # populate the data
    sys.stderr.write("Table Size "+str(rowcount)+', '+str(freq))
    ins_sql = """
        INSERT INTO """+dbname+"""."""+tablename+""" (v1,v2)
            SELECT
              random(0,10000000) || random(0,10000000) || random(0,10000000) || random(0,10000000) || random(0,10000000)
              || 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' as v1
              ,random(0,"""+str(freq)+""") as v2
            FROM
               sys_calendar.calendar c1
              , sys_calendar.calendar c2
            WHERE
              c1.day_of_calendar between 1 and """+str(rowcount)+"""
                 and c2.day_of_calendar between 1 and 1
            ;
        """
    #print ("Table Command", ins_sql)

    try:
        con.execute(ins_sql)
        output_table = ['OK']
    except:
        output_table = ['Fail']
    return output_table

def JoinTables(dbname, tablename1, tablename2, index):
    # Establish connection
    Vantage = 'tdap1627t2.labs.teradata.com'
    User = 'alice'
    Pass = 'alice'
    sys.stderr.write("Starting to connect\n")
    con = create_context(Vantage, User, Pass)

    # Input constants for now
    run = '7_30_1'
    # Join without index
    try:
        sys.stderr.write("Dropping table\n")
        con.execute("drop table tempdata")
    except Exception as e:
        pass
    con.execute("SET Query_Band='run=#jdf_8_83_"+str(index)+"_no_statistics;' FOR SESSION;")
    #data1 = DataFrame.from_table(dbname+"."+tablename1)
    #data2 = DataFrame.from_table(dbname+"."+tablename1)
    #join = data1.join(other = data2, on = ["v2"], lsuffix = "t1", rsuffix = "t2")
    sys.stderr.write("Joining1\n")
    con.execute("create multiset table tempdata as (select count(*) as test from "+dbname+"."+tablename1+" d1, "+dbname+"."+tablename2+" d2 where d1.v2=d2.v2) with data no primary index")
    con.execute("SET Query_Band='run=#jdf_8_00_"+str(index)+"_no_statistics;' FOR SESSION;")
    #result=join.count()
    con.execute("drop table tempdata")
    data1=None
    data2=None
    #print(result)
    sys.stderr.write("Joining1 complete\n")
    remove_context()
    con = create_context(Vantage, User, Pass)

    # Create Index
    sys.stderr.write("Collecting\n")
    con.execute("SET Query_Band='run=#jdf_8_00_"+str(index)+"_no_statistics;' FOR SESSION;")
    con.execute("collect statistics column v2 on "+dbname+"."+tablename1)
    con.execute("collect statistics column v2 on "+dbname+"."+tablename2)

    # Join with index
    con.execute("SET Query_Band='run=#jdf_8_84_"+str(index)+"_collected_statistics;' FOR SESSION;")
    #data1 = DataFrame.from_table(dbname+"."+tablename1)
    #data2 = DataFrame.from_table(dbname+"."+tablename2)
    #join = data1.join(other = data2, on = ["v2"], lsuffix = "t1", rsuffix = "t2")
    con.execute("create multiset table tempdata as (select count(*) as test from "+dbname+"."+tablename1+" d1, "+dbname+"."+tablename2+" d2 where d1.v2=d2.v2) with data no primary index")
    con.execute("SET Query_Band='run=#jdf_8_00_"+str(index)+"_no_statistics;' FOR SESSION;")
    #result=join.count()
    con.execute("drop table tempdata")
    sys.stderr.write("Joining2\n")
    #join=DataFrame.from_query("select count(*) as test2 from alice.data1 d1, alice.data2 d2 where d1.v2=d2.v2")
    #result=join.count()
    con.execute("SET Query_Band='run=#jdf_8_00_"+str(index)+"_no_statistics;' FOR SESSION;")
    #print(result)
    sys.stderr.write("Joining2 complete\n")
    con.execute("FLUSH QUERY LOGGING WITH ALL;")

    # Finish
    sys.stderr.write("Finishing\n")
    output_table = DataFrame.from_query("select 1 as OK").to_pandas()
    return output_table

def PredictSQL():
    # Establish connection
    Vantage = 'tdap1627t2.labs.teradata.com'
    User = 'alice'
    Pass = 'alice'
    sys.stderr.write("Starting to connect\n")
    con = create_context(Vantage, User, Pass)

    # Input constants for now
    run = '7_30_1'
    # Join without index
    try:
        sys.stderr.write("Dropping table prediction_sentiment\n")
        con.execute("drop table prediction_sentiment;")
    except:
        pass

    sys.stderr.write("Analyzing sentiment\n")
    con.execute("""
        create VOLATILE multiset table prediction_sentiment as (
        select
          queryid
        , out_polarity
        , out_strength
        , join_condition
        from
        (
        SELECT * FROM SentimentExtractor (
          ON (
                select
                  d.Queryid as queryid
                , e.ExplainText as explaintext
                , d.collecttimestamp
                , position('joined using' in e.explaintext) as joinstart
                , substr(e.explaintext,joinstart) as JoinText
                , position(',' in JoinText) as JoinEnd
                , SUBSTR(e.explaintext,joinstart+length('joined using,'),JoinEnd-12) JoinType
                , CASE WHEN JoinType like '%merge%' THEN '1'
                     WHEN JoinType like '%product%' THEN '2'
                     WHEN JoinType like '%dynamic hash%' THEN '3'
                            ELSE '0' END AS join_condition
                from
                dbc.dbqlogtbl d, dbc.dbqlexplaintbl e
                where
                d.QueryBand like '%jdf_8_8%'
                and d.QueryText like '%SELECT%'
                and d.QueryID = e.QueryID
                and JoinStart>0
                -- where
                -- collecttimestamp > '2019-08-06 09:00:00'
        ) PARTITION BY ANY
        ON dbql_sentiment AS dict DIMENSION
          USING
          TextColumn ('explaintext')
          AnalysisType ('document')
          Accumulate ('queryid','join_condition')
        ) AS dt
        ) as sentiment
        ) with data no primary index ON COMMIT PRESERVE ROWS
    """)

    try:
        con.execute("drop table target_collection")
    except:
        pass

    con.execute("""
        create  multiset table target_collection as (
        select
          objectdatabasename
        , objecttablename
        , objectcolumnname
        FROM
        (
            SELECT * FROM NaiveBayesPredict(
                ON "prediction_sentiment" AS "input"
                PARTITION BY ANY
                ON "ALICE".model_1 AS Model
                DIMENSION
                USING
                IdCol('queryid')
                Responses('stats_collected','need_collection')
                CategoricalInputs('out_polarity','join_condition')
            ) as sqlmr
         ) as dt
        , dbc.dbqlobjtbl d
        where
        d.queryid = dt.queryid
        and ObjectType='Col'
        group by 1,2,3
        ) with data no primary index
        --ON COMMIT PRESERVE ROWS
    """)

    target_collection = DataFrame.from_table("target_collection")

    for index, row in target_collection.to_pandas().iterrows():
        try:
            con.execute('collect statistics column '+row['ObjectTableName']+" on "+ \
                row['ObjectDatabaseName']+'.'+row['ObjectTableName'])
        except:
            pass

    output_table = target_collection.to_pandas()
    return output_table

def Predict():
    # Establish connection
    Vantage = 'tdap1627t2.labs.teradata.com'
    User = 'alice'
    Pass = 'alice'
    sys.stderr.write("Starting to connect\n")
    con = create_context(Vantage, User, Pass)
    sys.stderr.write("Connected\n")

    # Input constants for now
    run = '7_30_1'
    counter = '15'

    # Join without index
    dbqlog = DataFrame.from_table(in_schema("dbc", "dbqlogtbl")).drop("ZoneId", axis = 1)
    dbqlexplain = DataFrame.from_table(in_schema("dbc", "dbqlexplaintbl")).drop("ZoneID", axis = 1)
    dbqldata = dbqlog.join(other = dbqlexplain, on = ["QueryID"], lsuffix = "t1", rsuffix = "t2") \
        .select(['t1_QueryID','ExplainText','QueryBand','QueryText'])

    # Workaround until ELE-2072.
    dbqldata.to_sql('prediction_sentiment'+str(counter), if_exists="replace")
    dbqldata = DataFrame.from_table('prediction_sentiment'+str(counter))

    # Setup Categorization
    df_select_query_column_projection = [
         dbqldata.t1_QueryID.expression.label("queryid"),
         dbqldata.ExplainText.expression.label("explaintext"),
         dbqldata.QueryBand.expression.label("queryband"),
         func.REGEXP_SUBSTR(dbqldata.QueryBand.expression,
                            '(collected_statistics|no_statistics)', 1, 1, 'i').label("training"),
         func.REGEXP_SUBSTR(dbqldata.QueryText.expression,
                            'SELECT', 1, 1, 'i').label("select_info"),
         func.REGEXP_SUBSTR(func.REGEXP_SUBSTR(dbqldata.ExplainText.expression,
                            '(joined using a *[A-z \-]+ join,)', 1, 1, 'i'),
                                '[A-z]+', 15, 1, 'i').label("join_condition")]

    prediction_data = DataFrame.from_query(str(select(df_select_query_column_projection)
                                     #.where(Column('join_condition') != None)
                                     #.where(Column('training') != None)
                                     .compile(compile_kwargs={"literal_binds": True})))

    # Filter Data
    sys.stderr.write("Prepare training set\n")
    data_set = (prediction_data.join_condition != None)  & (prediction_data.training != None)
    prediction_set = prediction_data[data_set]

    prediction_set.to_sql('prediction_sentiment2'+str(counter), if_exists="replace")
    prediction_set = DataFrame.from_table('prediction_sentiment2'+str(counter))

    # Reference specific dictionary
    dictionary = DataFrame.from_table('dbql_sentiment')

    # Extract sentiment feature.
    predict = SentimentExtractor(
        dict_data = dictionary,
        newdata = prediction_set,
        level = "document",
        text_column = "explaintext",
        accumulate = ['queryid','join_condition','training']
    ).result

    # Reference model
    stats_model = DataFrame.from_table(in_schema("alice", "stats_model_final"))
    sys.stderr.write("Predicting\n")


    # Predict from queries columns needing collected statistics
    target_collection = NaiveBayesPredict(newdata=predict,
                                           modeldata = stats_model,
                                           formula="training ~ out_polarity + join_condition",
                                           id_col = "queryid",
                                           responses = ["collected_statistics","no_statistics"]
                                           ).result

    target_col = target_collection.assign(qid=target_collection.queryid).drop('queryid', axis=1)
    predictions = 'predictions' + str(counter)
    columnames = ['TableName']
    df = pd.DataFrame(columns=columnames)
    df.loc[0] = [predictions]
    target_col.to_sql(predictions, if_exists="replace")

    return df

def Collect(predictions):
    # Establish connection
    Vantage = 'tdap1627t2.labs.teradata.com'
    User = 'alice'
    Pass = 'alice'
    sys.stderr.write("Starting to connect\n")
    con = create_context(Vantage, User, Pass)
    sys.stderr.write("Connected\n")

    sys.stderr.write(predictions+'\n')
    target_col = DataFrame.from_table(predictions)

    # Reference the query's join data
    dbqlobj = DataFrame.from_table('dbc.dbqlobjtbl')
    sys.stderr.write("Setup to collect statistics\n")

    # Obtain query's join information
    target_names = target_col.join(other=dbqlobj, on=[target_col.qid == dbqlobj.QueryID], lsuffix="t1",
            rsuffix="t2").select(['ObjectDatabaseName', 'ObjectTableName', 'ObjectColumnName'])

    for index, row in target_names.to_pandas().iterrows():
        try:
            sys.stderr.write("Collecting on "+row['ObjectTableName']+'\n')
            con.execute('collect statistics column '+row['ObjectTableName']+" on "+ \
                row['ObjectDatabaseName']+'.'+row['ObjectTableName'])
        except:
            pass

    return target_names.to_pandas()
