"""
        2022 - Pietro Nardelli Mezzadri
        Class Responsible for dealing with the Database
        contains all SQL queries

"""

import sqlite3

class DBManager:
    def __init__(self):
        try:
            self.connection = sqlite3.connect("stat_keeper.db")
            self.cursor = self.connection.cursor()
        except Exception as e:
            print("Unable to establish connection with database")
            print(f"ERROR: {e}")
            
    def check_table_exists(self, table_name):
        try:
            sql = f"""
                SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';
            """
            return self.cursor.execute(sql).fetchone()
        except Exception as e:
            print("Unable to run sql")
            print(f"ERROR: {e}")

    def create_files_table(self):
        try:
            return self.cursor.execute("""CREATE TABLE FILE_HISTORY(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            FILENAME TEXT,
            UPLOADED_AT TEXT
            );""")
        except Exception as e:
            print("Unable to create files table")
            print(f"ERROR: {e}")
            
    def insert_file_data(self, file_name):
        try:
            sql = f"""
                INSERT INTO FILE_HISTORY (FILENAME, UPLOADED_AT)
                VALUES ("{file_name}", DATETIME('now'))
            """
            return self.cursor.execute(sql)
        except Exception as e:
            print("Unable to insert record into files table")
            print(f"ERROR: {e}")

    def get_files_uploaded(self, filename):
        try:
            return self.cursor.execute(f"""
                SELECT FILENAME FROM FILE_HISTORY
                WHERE FILENAME = "{filename}";
            """).fetchall()
        except Exception as e:
            print("Unable to get files")
            print(f"ERROR: {e}")
            
    def check_file_uploaded(self, file_name):
        try:
            sql = f"""
                SELECT FILE_NAME FROM GAME_STATS WHERE FILE_NAME='{file_name}';
            """
            return self.cursor.execute(sql).fetchone()
        except Exception as e:
            print("Unable to run sql")
            print(f"ERROR: {e}")
            
    def create_game_stats_table(self):
        try:
            return self.cursor.execute("""CREATE TABLE GAME_STATS(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            MATCH_DATE TEXT NOT NULL,
            MATCH_TIME TEXT NOT NULL,
            MATCH_SITE TEXT NOT NULL,
            MATCH_TEAM TEXT NOT NULL,
            MATCH_RIVAL TEXT NOT NULL,
            PLAYING_AWAY TEXT NOT NULL,
            PLAYER_NUMBER TEXT NOT NULL,
            PLAYER_NAME TEXT NOT NULL,
            PLAYER_TFG INTEGER NOT NULL,
            TOTAL_TFG INTEGER NOT NULL,
            PLAYER_2FG INTEGER NOT NULL,
            TOTAL_2FG INTEGER NOT NULL,
            PLAYER_3PT INTEGER NOT NULL,
            TOTAL_3PT INTEGER NOT NULL,
            PLAYER_FT INTEGER NOT NULL,
            TOTAL_FT INTEGER NOT NULL,
            PLAYER_PTS INTEGER NOT NULL,
            PLAYER_ORB INTEGER NOT NULL,
            PLAYER_DRB INTEGER NOT NULL,
            PLAYER_TR INTEGER NOT NULL,
            PLAYER_PF INTEGER NOT NULL,
            PLAYER_FD INTEGER NOT NULL,
            PLAYER_AST INTEGER NOT NULL,
            PLAYER_TO INTEGER NOT NULL,
            PLAYER_BS INTEGER NOT NULL,
            PLAYER_ST INTEGER NOT NULL,
            PLAYER_MIN_IN_SEC INTEGER NOT NULL,
            FILE_NAME TEXT NOT NULL,
            UPDATED_AT TEXT
            );""")
        except Exception as e:
            print("Unable to create files table")
            print(f"ERROR: {e}")
            
    def create_player_stat_table(self):
        try:
            return self.cursor.execute("""CREATE TABLE PLAYER_STATS(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            PLAYER_NUMBER TEXT NOT NULL,
            PLAYER_NAME TEXT NOT NULL,
            PLAYER_TEAM TEXT NOT NULL,
            _TFG TEXT NOT NULL,
            _2FG TEXT NOT NULL,
            _3PT TEXT NOT NULL,
            _FT TEXT NOT NULL,
            _ORB INTEGER NOT NULL,
            _ORB_PER_MATCH FLOAT NOT NULL,
            _PTS INTEGER NOT NULL,
            _PTS_PER_MATCH FLOAT NOT NULL,
            _DRB INTEGER NOT NULL,
            _DRB_PER_MATCH FLOAT NOT NULL,
            _TR INTEGER NOT NULL,
            _TR_PER_MATCH FLOAT NOT NULL,
            _PF INTEGER NOT NULL,
            _PF_PER_MATCH FLOAT NOT NULL,
            _FD INTEGER NOT NULL,
            _FD_PER_MATCH FLOAT NOT NULL,
            _AST INTEGER NOT NULL,
            _AST_PER_MATCH FLOAT NOT NULL,
            _TO INTEGER NOT NULL,
            _TO_PER_MATCH FLOAT NOT NULL,
            _BS INTEGER NOT NULL,
            _BS_PER_MATCH FLOAT NOT NULL,
            _ST INTEGER NOT NULL,
            _ST_PER_MATCH FLOAT NOT NULL,
            MIN_IN_SEC INTEGER NOT NULL,
            TOTAL_MATCHES INTEGER NOT NULL
            );""")
        except Exception as e:
            print("Unable to create files table")
            print(f"ERROR: {e}")
            
    def create_team_stat_table(self):
        try:
            return self.cursor.execute("""CREATE TABLE TEAM_STATS(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            TEAM_NAME TEXT NOT NULL,
            _TFG INTEGER NOT NULL,
            _2FG INTEGER NOT NULL,
            _3PT INTEGER NOT NULL,
            _FT INTEGER NOT NULL,
            _PTS INTEGER NOT NULL,
            _PTS_PER_MATCH FLOAT NOT NULL,
            _ORB INTEGER NOT NULL,
            _ORB_PER_MATCH FLOAT NOT NULL,
            _DRB INTEGER NOT NULL,
            _DRB_PER_MATCH FLOAT NOT NULL,
            _TR INTEGER NOT NULL,
            _TR_PER_MATCH FLOAT NOT NULL,
            _PF INTEGER NOT NULL,
            _PF_PER_MATCH FLOAT NOT NULL,
            _FD INTEGER NOT NULL,
            _FD_PER_MATCH FLOAT NOT NULL,
            _AST INTEGER NOT NULL,
            _AST_PER_MATCH FLOAT NOT NULL,
            _TO INTEGER NOT NULL,
            _TO_PER_MATCH FLOAT NOT NULL,
            _BS INTEGER NOT NULL,
            _BS_PER_MATCH FLOAT NOT NULL,
            _ST INTEGER NOT NULL,
            _ST_PER_MATCH FLOAT NOT NULL,
            MIN_IN_SEC INTEGER NOT NULL,
            TOTAL_MATCHES INTEGER NOT NULL
            );""")
        except Exception as e:
            print("Unable to create files table")
            print(f"ERROR: {e}")
            
            
    def insert_player_stats(self, player_stats):
      try:
        sql = f"""
          INSERT INTO PLAYER_STATS
          (PLAYER_NUMBER, PLAYER_NAME, PLAYER_TEAM, _TFG, _2FG, _3PT,
          _FT, _ORB, _ORB_PER_MATCH, _PTS, _PTS_PER_MATCH, _DRB, _DRB_PER_MATCH,
          _TR, _TR_PER_MATCH, _PF, _PF_PER_MATCH, _FD, _FD_PER_MATCH, _AST, _AST_PER_MATCH,
          _TO, _TO_PER_MATCH, _BS, _BS_PER_MATCH, _ST, _ST_PER_MATCH, MIN_IN_SEC, TOTAL_MATCHES)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?)
        """
        return self.cursor.execute(sql, player_stats)
      except Exception as e:
          print("Unable to insert record into table")
          print(f"ERROR: {e}")
          print(player_stats)
          
          
    def insert_team_stats(self, team_stats):
      try:
        sql = f"""
          INSERT INTO TEAM_STATS
          (TEAM_NAME, _TFG, _2FG, _3PT,
          _FT, _ORB, _ORB_PER_MATCH, _PTS, _PTS_PER_MATCH, _DRB, _DRB_PER_MATCH,
          _TR, _TR_PER_MATCH, _PF, _PF_PER_MATCH, _FD, _FD_PER_MATCH, _AST, _AST_PER_MATCH,
          _TO, _TO_PER_MATCH, _BS, _BS_PER_MATCH, _ST, _ST_PER_MATCH, MIN_IN_SEC, TOTAL_MATCHES)
          VALUES (?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, ?, ?)
        """
        return self.cursor.execute(sql, team_stats)
      except Exception as e:
          print("Unable to insert record into table")
          print(f"ERROR: {e}")
          print(team_stats)
          
          
    def delete_player_stat_table(self):
      try:
        sql = f"""
          DROP TABLE PLAYER_STATS
        """
        return self.cursor.execute(sql)
      except Exception as e:
          print("Unable to delete table")
          print(f"ERROR: {e}")
          
    def delete_team_stat_table(self):
      try:
        sql = f"""
          DROP TABLE TEAM_STATS
        """
        return self.cursor.execute(sql)
      except Exception as e:
          print("Unable to delete table")
          print(f"ERROR: {e}")
    

    def insert_game_stats(self, player_stats):
      try:
        sql = f"""
          INSERT INTO GAME_STATS
          (MATCH_DATE, MATCH_TIME, MATCH_SITE, MATCH_TEAM, MATCH_RIVAL,
          PLAYING_AWAY, PLAYER_NUMBER, PLAYER_NAME, PLAYER_TFG, TOTAL_TFG,
          PLAYER_2FG, TOTAL_2FG, PLAYER_3PT, TOTAL_3PT, PLAYER_FT,
          TOTAL_FT, PLAYER_PTS, PLAYER_ORB, PLAYER_DRB, PLAYER_TR,
          PLAYER_PF, PLAYER_FD, PLAYER_AST, PLAYER_TO, PLAYER_BS, 
          PLAYER_ST, PLAYER_MIN_IN_SEC, FILE_NAME, UPDATED_AT)
          VALUES (?, ?, ?, ?, ?,
                  ?, ?, ?, ?, ?, 
                  ?, ?, ?, ?, ?, 
                  ?, ?, ?, ?, ?, 
                  ?, ?, ?, ?, ?, 
                  ?, ?, ?, DATETIME('now'))
        """
        return self.cursor.execute(sql, player_stats)
      except Exception as e:
          print("Unable to insert record into table")
          print(f"ERROR: {e}")
          print(player_stats)
    