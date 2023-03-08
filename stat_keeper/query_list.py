class QueryList:
  def __init__(self, db_manager):
    self.cursor = db_manager.cursor
  
  def get_all_teams(self):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT DISTINCT MATCH_TEAM FROM GAME_STATS;
        """).fetchall()
    
  def get_all_players(self):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT DISTINCT PLAYER_NUMBER, MATCH_TEAM FROM GAME_STATS;
        """).fetchall()
    
  def get_all_players_from_team(self, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT DISTINCT PLAYER_NUMBER, PLAYER_NAME FROM GAME_STATS
        WHERE MATCH_TEAM = ?;
        """, [team_name]).fetchall()
    
  def get_player_total(self, player_name, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT PLAYER_NUMBER, PLAYER_NAME, MATCH_TEAM,
        (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), SUM(PLAYER_ORB), SUM(PLAYER_DRB), SUM(PLAYER_TR),
        SUM(PLAYER_PF), SUM(PLAYER_FD), SUM(PLAYER_AST), SUM(PLAYER_TO), SUM(PLAYER_BS), 
        SUM(PLAYER_ST), SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE PLAYER_NUMBER LIKE ? AND MATCH_TEAM LIKE ?
        GROUP BY PLAYER_NUMBER;
        """, [player_name, team_name]).fetchall()
    
  def get_team_total(self, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT MATCH_TEAM, (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), SUM(PLAYER_ORB), SUM(PLAYER_DRB), SUM(PLAYER_TR),
        SUM(PLAYER_PF), SUM(PLAYER_FD), SUM(PLAYER_AST), SUM(PLAYER_TO), SUM(PLAYER_BS), 
        SUM(PLAYER_ST), SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE MATCH_TEAM LIKE ?
        GROUP BY MATCH_TEAM;
        """, [team_name]).fetchall()
    
  def get_player_total_by_team(self, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT MATCH_TEAM, (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), SUM(PLAYER_ORB), SUM(PLAYER_DRB), SUM(PLAYER_TR),
        SUM(PLAYER_PF), SUM(PLAYER_FD), SUM(PLAYER_AST), SUM(PLAYER_TO), SUM(PLAYER_BS), 
        SUM(PLAYER_ST), SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE MATCH_TEAM LIKE ?
        GROUP BY MATCH_TEAM;
        """, [team_name]).fetchall()
    
  def get_player_total_per_match(self, player_name, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT PLAYER_NUMBER, PLAYER_NAME, MATCH_TEAM,
        (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), CAST(SUM(PLAYER_PTS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ORB), 
        CAST(SUM(PLAYER_ORB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_DRB), CAST(SUM(PLAYER_DRB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TR), 
        CAST(SUM(PLAYER_TR) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT),
        SUM(PLAYER_PF), CAST(SUM(PLAYER_PF) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_FD), CAST(SUM(PLAYER_FD) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_AST), 
        CAST(SUM(PLAYER_AST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TO), CAST(SUM(PLAYER_TO) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_BS), 
        CAST(SUM(PLAYER_BS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ST), CAST(SUM(PLAYER_ST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), 
        SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE PLAYER_NUMBER LIKE ? AND MATCH_TEAM LIKE ?
        GROUP BY PLAYER_NUMBER;
        """, [player_name, team_name]).fetchall()
    
  def get_team_total_per_match(self, team_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT MATCH_TEAM, (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), CAST(SUM(PLAYER_PTS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ORB), 
        CAST(SUM(PLAYER_ORB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_DRB), CAST(SUM(PLAYER_DRB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TR), 
        CAST(SUM(PLAYER_TR) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT),
        SUM(PLAYER_PF), CAST(SUM(PLAYER_PF) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_FD), CAST(SUM(PLAYER_FD) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_AST), 
        CAST(SUM(PLAYER_AST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TO), CAST(SUM(PLAYER_TO) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_BS), 
        CAST(SUM(PLAYER_BS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ST), CAST(SUM(PLAYER_ST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE MATCH_TEAM LIKE ?
        GROUP BY MATCH_TEAM;
        """, [team_name]).fetchall()
    
  def get_team_total_per_match_rival_team(self, team_name, rival_name):
    #self.cursor.row_factory = lambda cursor, row: row[0]
    return self.cursor.execute("""
        SELECT MATCH_TEAM, MATCH_RIVAL, (SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG)), SUM(PLAYER_2FG) || '/' || SUM(TOTAL_2FG), (SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT)), 
        (SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT)), SUM(PLAYER_PTS), CAST(SUM(PLAYER_PTS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ORB), 
        CAST(SUM(PLAYER_ORB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_DRB), CAST(SUM(PLAYER_DRB) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TR), 
        CAST(SUM(PLAYER_TR) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT),
        SUM(PLAYER_PF), CAST(SUM(PLAYER_PF) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_FD), CAST(SUM(PLAYER_FD) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_AST), 
        CAST(SUM(PLAYER_AST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_TO), CAST(SUM(PLAYER_TO) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_BS), 
        CAST(SUM(PLAYER_BS) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_ST), CAST(SUM(PLAYER_ST) AS FLOAT)/CAST(COUNT(DISTINCT FILE_NAME) AS FLOAT), SUM(PLAYER_MIN_IN_SEC), COUNT(DISTINCT FILE_NAME)
        FROM GAME_STATS
        WHERE MATCH_TEAM LIKE ? AND MATCH_RIVAL LIKE ?
        GROUP BY MATCH_TEAM;
        """, [team_name, rival_name]).fetchall()


  def get_promediodetres(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT), CAST(CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY PLAYER_NAME
      ORDER BY CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) DESC;                
    """).fetchmany(10)

  # def get_promediodetres(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT), CAST(CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME
  #     ORDER BY SUM(PLAYER_3PT) DESC;                 
  #   """).fetchmany(10)
       

    
  def get_teampromediodetres(self):
    return self.cursor.execute("""
      SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT), CAST(CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY MATCH_TEAM 
      ORDER BY CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) DESC;                   
    """).fetchmany(10)
    
  # def get_teampromediodetres(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_3PT) || '/' || SUM(TOTAL_3PT), CAST(CAST(SUM(PLAYER_3PT) AS FLOAT) / CAST(SUM(TOTAL_3PT) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_3PT) DESC;                   
  #   """).fetchmany(10)

  def get_rebotes(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, PLAYER_TEAM,TOTAL_MATCHES, _TR, CAST(_TR_PER_MATCH AS INT)
      FROM PLAYER_STATS
      GROUP BY PLAYER_NAME 
      ORDER BY (_TR) DESC;                   
    """).fetchmany(10)
        
  # def get_rebotes(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_TR)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME 
  #     ORDER BY SUM(PLAYER_TR) DESC;                   
  #   """).fetchmany(10)

  def get_teamrebotes(self):
    return self.cursor.execute("""
      SELECT TEAM_NAME, TOTAL_MATCHES, _TR, CAST(_TR_PER_MATCH AS INT)
      FROM TEAM_STATS
      GROUP BY TEAM_NAME 
      ORDER BY _TR DESC;                   
    """).fetchmany(10)    
  # def get_teamrebotes(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_TR)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_TR) DESC;                   
  #   """).fetchmany(10)

  def get_tiroslibres(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY PLAYER_NAME
      ORDER BY CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) DESC;                 
    """).fetchmany(10)

  # def get_tiroslibres(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME
  #     ORDER BY SUM(PLAYER_FT) DESC;                 
  #   """).fetchmany(10)

  def get_teamtiroslibres(self):
    return self.cursor.execute("""
      SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY MATCH_TEAM 
      ORDER BY CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) DESC;                   
    """).fetchmany(10)

  # def get_teamtiroslibres(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_FT) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_FT) AS FLOAT) / CAST(SUM(TOTAL_FT) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_FT) DESC;                   
  #   """).fetchmany(10)


  def get_anotaciones(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_PTS), CAST(SUM(PLAYER_PTS) AS INT) / CAST(COUNT(DISTINCT FILE_NAME) AS INT)
      FROM GAME_STATS
      GROUP BY PLAYER_NAME 
      ORDER BY SUM(PLAYER_PTS) DESC;                   
    """).fetchmany(10)



  def get_teamanotaciones(self):
    return self.cursor.execute("""
      SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_PTS),  CAST(SUM(PLAYER_PTS) AS INT) / CAST(COUNT(DISTINCT FILE_NAME) AS INT)
      FROM GAME_STATS
      GROUP BY MATCH_TEAM 
      ORDER BY SUM(PLAYER_PTS) DESC;                   
    """).fetchmany(10) 

  def get_asistencias(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, PLAYER_TEAM, TOTAL_MATCHES, _AST, CAST(_AST_PER_MATCH AS INT)
      FROM PLAYER_STATS
      GROUP BY PLAYER_NAME 
      ORDER BY SUM(_AST) DESC;                   
    """).fetchmany(10)

  # def get_asistencias(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_AST)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME 
  #     ORDER BY SUM(PLAYER_AST) DESC;                   
  #   """).fetchmany(10)

  def get_teamasistencias(self):
    return self.cursor.execute("""
      SELECT TEAM_NAME, TOTAL_MATCHES, _AST, CAST(_AST_PER_MATCH AS INT)
      FROM TEAM_STATS
      GROUP BY TEAM_NAME
      ORDER BY SUM(_AST) DESC;                   
    """).fetchmany(10)

  # def get_teamasistencias(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_AST)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_AST) DESC;                   
  #   """).fetchmany(10)

  def get_steals(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, PLAYER_TEAM, TOTAL_MATCHES, _ST, CAST(_ST_PER_MATCH AS INT)
      FROM PLAYER_STATS
      GROUP BY PLAYER_NAME 
      ORDER BY SUM(_ST) DESC;                   
    """).fetchmany(10)

  # def get_steals(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_ST)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME 
  #     ORDER BY SUM(PLAYER_ST) DESC;                   
  #   """).fetchmany(10)

  def get_teamsteals(self):
    return self.cursor.execute("""
      SELECT TEAM_NAME, TOTAL_MATCHES, _ST, CAST(_ST_PER_MATCH AS INT)
      FROM TEAM_STATS
      GROUP BY TEAM_NAME 
      ORDER BY SUM(_ST) DESC;                   
    """).fetchmany(10)

  # def get_teamsteals(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_ST)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_ST) DESC;                   
  #   """).fetchmany(10)

  def get_bloqueos(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, PLAYER_TEAM, TOTAL_MATCHES, _BS, CAST(_BS_PER_MATCH AS INT)
      FROM PLAYER_STATS
      GROUP BY PLAYER_NAME 
      ORDER BY SUM(_BS) DESC;                   
    """).fetchmany(10)
       
  # def get_bloqueos(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_BS)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME 
  #     ORDER BY SUM(PLAYER_BS) DESC;                   
  #   """).fetchmany(10)
    
  def get_teambloqueos(self):
    return self.cursor.execute("""
      SELECT TEAM_NAME, TOTAL_MATCHES, _BS, CAST(_BS_PER_MATCH AS INT)
      FROM TEAM_STATS
      GROUP BY TEAM_NAME 
      ORDER BY SUM(_BS) DESC;                   
    """).fetchmany(10)  
  # def get_teambloqueos(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_BS)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_BS) DESC;                   
  #   """).fetchmany(10)

  def get_prompuntos(self):
    return self.cursor.execute("""
      SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_TFG) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY PLAYER_NAME
      ORDER BY CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) DESC;                 
    """).fetchmany(10)    
  # def get_prompuntos(self):
  #   return self.cursor.execute("""
  #     SELECT PLAYER_NAME, MATCH_TEAM, COUNT(DISTINCT FILE_NAME), SUM(PLAYER_TFG) || '/' || SUM(TOTAL_FT), CAST(CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY PLAYER_NAME
  #     ORDER BY SUM(PLAYER_TFG) DESC;                 
  #   """).fetchmany(10)

  def get_teamprompuntos(self):
    return self.cursor.execute("""
      SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG), CAST(CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) * 100 AS INT)
      FROM GAME_STATS
      GROUP BY MATCH_TEAM 
      ORDER BY CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) DESC;                   
    """).fetchmany(10)    
  # def get_teamprompuntos(self):
  #   return self.cursor.execute("""
  #     SELECT MATCH_TEAM, COUNT(DISTINCT FILE_NAME),SUM(PLAYER_TFG) || '/' || SUM(TOTAL_TFG), CAST(CAST(SUM(PLAYER_TFG) AS FLOAT) / CAST(SUM(TOTAL_TFG) AS FLOAT) * 100 AS INT)
  #     FROM GAME_STATS
  #     GROUP BY MATCH_TEAM 
  #     ORDER BY SUM(PLAYER_TFG) DESC;                   
  #   """).fetchmany(10)