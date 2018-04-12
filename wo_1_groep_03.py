def query_01(connection, column_names):
    # Bouw je query
    query="""
    SELECT t.name, t.yearID, t.HR
    FROM Teams as t
    ORDER BY t.HR DESC;
    """
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_02(connection, column_names, datum = '1980-01-16'):
    # Bouw je query
    query="""
    SELECT p.nameFirst, p.nameLast, p.birthYear, p.birthMonth, p.birthDay
    FROM Master as p
    WHERE STR_TO_DATE(p.debut, '%Y-%m-%d') > STR_TO_DATE({}, '%Y-%m-%d')
    ORDER BY p.nameLast ASC;
    """.format(datum)
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_03(connection, column_names):
    # Bouw je query
    query="""
    SELECT DISTINCT t.name, p.nameFirst, p.nameLast
    FROM     Teams as t, 
             Managers as m,
             Master as p
    WHERE    m.plyrMgr = 'N' AND p.playerID = m.playerID
    ORDER BY t.name;
    """
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_04(connection, column_names, datum_x='1980-01-01', datum_y='1980-01-01'):
    # Bouw je query
    query="""
    SELECT  t.name, t.rank, t.W, t.L, p.nameFirst, p.nameLast
    
    FROM    Teams as t, HallOfFame as h, Managers as m, Master as p
    
    WHERE   p.playerID = m.playerID AND
            m.playerID = h.playerID AND 
            h.yearID > {} AND     
            m.teamID = t.teamID AND 
            m.yearID > {}              
 
    ORDER BY t.name;
    """.format(datum_x, datum_y)
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_05(connection, column_names):
    # Bouw je query
    query="""
    SELECT DISTINCT t.name
    FROM Teams as t 
    WHERE (SELECT COUNT(*)
       FROM Managers as m 
       WHERE m.teamID = t.teamID 
       AND m.yearID > 1980 
       AND m.plyrMgr = 'Y') > 0 
    ORDER BY t.name;
    """
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_06(connection, column_names, salaris=20000):
    # Bouw je query
    query="""
    SELECT DISTINCT t.name, t.Rank, t.yearID, t.W, t.L
    FROM Teams as t
    WHERE NOT EXISTS
        (SELECT *
        FROM salaries as s
        WHERE s.yearID = t.yearID
            AND s.teamID = t.teamID
            AND s.salary < {})
    ORDER BY t.W ASC;
    """.format(salaris)
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_07(connection, column_names):
    # Bouw je query
    query="""
    SELECT DISTINCT m.nameLast, m.nameFirst 
    
    FROM master as m
    WHERE   (SELECT COUNT(DISTINCT playerID, awardID)
            FROM  awardsmanagers as aw
            WHERE m.playerID = aw.playerID)
        =
            (SELECT COUNT(DISTINCT awardID)
            FROM awardsmanagers as aw)
    ORDER BY m.nameLast ASC;
    """
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_08(connection, column_names, jaar=1990, lengte=75):
    # Bouw je query
    query="""
    SELECT p.birthState, AVG(p.weight), AVG(p.height), AVG(bat.HR), AVG(pitch.SV)
    FROM Master as p, Batting as bat, Pitching as pitch, HallOfFame as hof
    WHERE p.playerID = pitch.playerID AND p.playerID = bat.playerID
        AND p.playerID = hof.playerID AND hof.yearid > {} AND hof.inducted = 'Y'
    GROUP BY p.birthState
    HAVING AVG(p.height) > {}
    ORDER BY p.birthState ASC 
    """.format(jaar, lengte)
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df

def query_09(connection, column_names, jaar=1975):
    # Bouw je query
    query="""
    SELECT t1.yearID, t1.name, t1.HR
    FROM   Teams as t1
    WHERE  (SELECT COUNT(*)
            FROM   Teams as t2
            WHERE  t1.HR < t2.HR)
    = 1;
    """
    
    # Stap 2 & 3
    res = run_query(connection, query)         # Query uitvoeren
    df = res_to_df(res, column_names)          # Query in DataFrame brengen
    
    return df