"""
        2022 - Pietro Nardelli Mezzadri
        Class Responsible for most of the operation
        Data Extraction from the PDF file
        Data Handling and insertion or update into the database
"""

import os, pathlib
from stat_keeper.csv_handler import CsvHandler
from stat_keeper.pdf_file import PdfFile
from database.db_manager import DBManager
from stat_keeper.query_list import QueryList

class StatKeeper:
    def __init__(self) -> None:
        self.pdf_directory = str(pathlib.Path(__file__).parent.parent.resolve()) + "\pdf"
        self.db_manager = DBManager()
        self.query_list = QueryList(self.db_manager)
        self.csv_handler = CsvHandler()
        try:
            os.path.isdir(self.pdf_directory)
        except Exception as e:
            print("Unable to find PDF directory")
            print(f"ERROR: {e}")
            print(f"PDF dir: {self.pdf_directory}")

    # list all PDF files in the pdf directory
    def get_pdf_files(self):
        return os.listdir(self.pdf_directory)

    # extract PDF contents
    def get_pdf_contents(self, pdf_file: str):
        pdf_file = PdfFile(self.pdf_directory + "\\" + pdf_file)
        return pdf_file.get_page(0)

    # extract player stats from PDF
    def extract_data(self, page, team_number):
        return list(filter(None, page[0].split("MIN")[team_number].split("Team")[0].split("\n")))

    # create database in case it does not exist
    def setup(self):
        if not self.db_manager.check_table_exists("FILE_HISTORY"):
            self.db_manager.create_files_table()
        if not self.db_manager.check_table_exists("GAME_STATS"):
          self.db_manager.create_game_stats_table()
        self.db_manager.connection.commit()
        self.db_manager.connection.close()
            
    def extract_match_date(self, pdf_content):
      return pdf_content[0].split("Date:")[1].split("Time")[0].strip()
    
    def extract_match_time(self, pdf_content):
      return pdf_content[0].split("Time:")[1].split("Site")[0].strip()
    
    def extract_match_site(self, pdf_content):
      return pdf_content[0].split("Site:")[1].split("Attendance")[0].strip()
    
    def split_player_data(self, players_data):
      player_data = []
      temp_data = []
      column_num = 0
      for stat in players_data:
        # skipping AA
        if column_num == 13:
          pass
        elif "/" in stat:
          temp_data.append(stat.split("/")[0])
          temp_data.append(stat.split("/")[1])
        elif ":" in stat:
          try:
            total_min = int(stat.split(":")[0])*60 + int(stat.split(":")[1])
          except Exception as e:
            temp_data.append(stat.split()[0])
            total_min = int(stat.split()[1].split(":")[0])*60 + int(stat.split()[1].split(":")[1])
          temp_data.append(total_min)
        elif "DNP" in stat:
          temp_data.append(stat.split()[0])
          temp_data.append(0)
        else:
          temp_data.append(stat)
        
        column_num += 1
        
        if ":" in stat or "DNP" in stat:
          player_data.append(temp_data)
          temp_data = []
          column_num = 0
      return player_data
    
    def extract_match_data(self, pdf):
      pdf_content = self.get_pdf_contents(pdf)
      player_data = []
      for team_number in range(1,3):
        team = ["Visitors: ", "Home: "]
        match_date = self.extract_match_date(pdf_content)
        match_time = self.extract_match_time(pdf_content)
        match_site = self.extract_match_site(pdf_content)
        if team_number == 1:
          team_name = pdf_content[0].split("Visitors: ")[1].split("\n")[0].strip()
          rival_name = pdf_content[0].split("Home: ")[1].split("\n")[0].strip()
        elif team_number == 2:
          team_name = pdf_content[0].split("Home: ")[1].split("\n")[0].strip()
          rival_name = pdf_content[0].split("Visitors: ")[1].split("\n")[0].strip()
        info = self.extract_data(pdf_content, team_number)
        player_stats = self.split_player_data(info)
        for player in player_stats:
          player_data.append(match_date)
          player_data.append(match_time)
          player_data.append(match_site)
          player_data.append(team_name)
          player_data.append(rival_name)
          player_data.append(team_number == 1)
          for stat in player:
            player_data.append(stat)
          player_data.append(pdf)
          self.db_manager.insert_game_stats(player_data)
          self.db_manager.connection.commit()
          player_data = []
          
    def get_all_teams(self):
        return self.query_list.get_all_teams()
        
    def get_all_players(self):
        return self.query_list.get_all_players()
        
    def get_player_total(self, player_name, team_name):
        return self.query_list.get_player_total(player_name, team_name)
      
    def get_players_total(self):
        player_list = self.get_all_players()
        players_total = []
        for player in player_list:
          players_total.append(self.query_list.get_player_total(player[0], player[1])[0])
        sorted_list = sorted(players_total, key=lambda x: x[4], reverse=True)[:10]
        return sorted_list
      
    def get_players_total_by_team(self, team_name):
      player_list = self.query_list.get_all_players_from_team(team_name)
      players_total = []
      for player in player_list:
        players_total.append(self.query_list.get_player_total(player[0], team_name)[0])
      sorted_list = sorted(players_total, key=lambda x: x[4], reverse=True)[:10]
      return sorted_list
    
    def get_players_total_per_match_by_team(self, team_name):
      player_list = self.query_list.get_all_players_from_team(team_name)
      players_total = []
      for player in player_list:
        player_total = list(self.query_list.get_player_total_per_match(player[0], team_name)[0])
        del player_total[2]
        players_total.append(player_total)
      return players_total
      
      
    def get_players_total_per_match(self):
        player_list = self.get_all_players()
        players_total = []
        for player in player_list:
          players_total.append(self.query_list.get_player_total_per_match(player[0], player[1])[0])
        #sorted_list = sorted(players_total, key=lambda x: x[4], reverse=True)[:10]
        return players_total
    
    def get_team_total(self, team_name):
      team_total = self.query_list.get_team_total(team_name)
      return team_total
    
    def get_teams_total(self):
      team_list = self.get_all_teams()
      teams_total = []
      for team in team_list:
        team_data = self.query_list.get_team_total(team[0])
        teams_total.append(team_data[0])
      sorted_list = sorted(teams_total, key=lambda x: x[4], reverse=True)[:10]
      return sorted_list
    
    def get_teams_total_per_match(self):
      team_list = self.get_all_teams()
      teams_total = []
      for team in team_list:
        team_data = self.query_list.get_team_total_per_match(team[0])
        teams_total.append(team_data[0])
      return teams_total
    
    def get_team_total_per_match_rival_team(self, team_name, rival_name):
      team_total = self.query_list.get_team_total_per_match_rival_team(team_name, rival_name)
      return team_total
        
    def export_player_data_to_csv(self):
      headers = ["NUMBER", "NAME", "TEAM", "TFG", "2FG", "3PT", "FT",
          "PTS", "ORB", "DRB", "TR", "PF", "FD", "AST", "TO", 
          "BS", "ST", "MIN_IN_SEC", "TOTAL_MATCHES"]
      values = self.get_players_total()
      self.csv_handler.open_file("csv/players_total.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def export_player_data_per_match_to_csv(self):
      headers = ["NUMBER", "NAME", "TEAM", "TFG", "2FG", "3PT", "FT",
          "PTS", "PTS/MATCH", "ORB", "ORB/MATCH", "DRB", "DRB/MATCH", "TR", "TR/MATCH", "PF", "PF/MATCH", "FD", "FD/MATCH", "AST", "AST/MATCH", "TO", "TO/MATCH", 
          "BS", "BS/MATCH", "ST", "ST/MATCH", "MIN_IN_SEC", "TOTAL_MATCHES"]
      values = self.get_players_total_per_match()
      self.csv_handler.open_file("csv/players_total_per_match.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def export_team_data_to_csv(self):
      headers = ["TEAM", "TFG", "2FG", "3PT", "FT",
          "PTS", "ORB", "DRB", "TR", "PF", "FD", "AST", "TO", 
          "BS", "ST", "MIN_IN_SEC", "TOTAL_MATCHES"]
      values = self.get_teams_total()
      self.csv_handler.open_file("csv/teams_total.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def export_team_data_per_match_to_csv(self):
      headers = ["TEAM", "TFG", "2FG", "3PT", "FT",
          "PTS", "PTS/MATCH", "ORB", "ORB/MATCH", "DRB", "DRB/MATCH", "TR", "TR/MATCH", "PF", "PF/MATCH", "FD", "FD/MATCH", "AST", "AST/MATCH", "TO", "TO/MATCH", 
          "BS", "BS/MATCH", "ST", "ST/MATCH", "MIN_IN_SEC", "TOTAL_MATCHES"]
      values = self.get_teams_total_per_match()
      self.csv_handler.open_file("csv/teams_total_per_match.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def export_team_data_rival_to_csv(self, team_name, rival_name):
      headers = ["TEAM", "RIVAL", "TFG", "2FG", "3PT", "FT",
          "PTS", "PTS/MATCH", "ORB", "ORB/MATCH", "DRB", "DRB/MATCH", "TR", "TR/MATCH", "PF", "PF/MATCH", "FD", "FD/MATCH", "AST", "AST/MATCH", "TO", "TO/MATCH", 
          "BS", "BS/MATCH", "ST", "ST/MATCH", "MIN_IN_SEC", "TOTAL_MATCHES"]
      values = self.get_team_total_per_match_rival_team(team_name, rival_name)
      self.csv_handler.open_file(f"csv/{team_name}vs{rival_name}.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_promediodetres(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_promediodetres()
      self.csv_handler.open_file(f"csv/players/promediodetres.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teampromediodetres(self):
      headers = ['Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_teampromediodetres()
      self.csv_handler.open_file(f"csv/players/promediodetres(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_rebotes(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Rebotes', 'Promedio']
      values = self.query_list.get_rebotes()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[3]/value[2]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/rebotes.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamrebotes(self):
      headers = ['Equipo', 'Juegos Jugados','Rebotes', 'Promedio']
      values = self.query_list.get_teamrebotes()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[2]/value[1]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/rebotes(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_tiroslibres(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_tiroslibres()
      self.csv_handler.open_file(f"csv/players/promediodetiroslibres.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamtiroslibres(self):
      headers = ['Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_teamtiroslibres()
      self.csv_handler.open_file(f"csv/players/promediodetiroslibres(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_anotaciones(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_anotaciones()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[3]/value[2]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/anotaciones.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamanotaciones(self):
      headers = ['Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_teamanotaciones()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[2]/value[1]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/anotaciones(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_asistencias(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Asistencias', 'Promedio']
      values = self.query_list.get_asistencias()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[3]/value[2]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/asistencias.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamasistencias(self):
      headers = ['Equipo', 'Juegos Jugados','Asistencias', 'Promedio']
      values = self.query_list.get_teamasistencias()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[2]/value[1]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/asistencias(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_steals(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Robos de balon', 'Promedio']
      values = self.query_list.get_steals()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[3]/value[2]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/steals.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamsteals(self):
      headers = ['Equipo', 'Juegos Jugados','Robos de balon', 'Promedio']
      values = self.query_list.get_teamsteals()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[2]/value[1]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/steals(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_bloqueos(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Bloqueos', 'Promedio']
      values = self.query_list.get_bloqueos()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[3]/value[2]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/bloqueos.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teambloqueos(self):
      headers = ['Equipo', 'Juegos Jugados','Bloqueos', 'Promedio']
      values = self.query_list.get_teambloqueos()
      result = []
      for value in list(values):
        stat = list(value)
        stat.append("{:.2f}".format(value[2]/value[1]))
        result.append(stat)
      self.csv_handler.open_file(f"csv/players/bloqueos(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_prompuntos(self):
      headers = ['Nombre', 'Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_prompuntos()
      self.csv_handler.open_file(f"csv/players/promediodepuntos.csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teamprompuntos(self):
      headers = ['Equipo', 'Juegos Jugados','Anotaciones', 'Promedio']
      values = self.query_list.get_teamprompuntos()
      self.csv_handler.open_file(f"csv/players/promediodepuntos(equipo).csv")
      self.csv_handler.write_row_headers(headers)
      self.csv_handler.write_row_values(values)
      self.csv_handler.close_file()
      
    def get_teams_stats(self):
      team_list = self.get_all_teams();
      headers = ['NUMERO', 'NOMBRE', 'TFG', '2FG', '3PT', 'FT', 
                 'PTS', 'PTS_PER_MATCH', 'ORB', 'ORB_PER_MATCH', 'DRB', 'DRB_PER_MATCH', 'TR', 'TR_PER_MATCH', 'PF', 'PF_PER_MATCH', 'FD', 'FD_PER_MATCH', 'AST', 'AST_PER_MATCH', 
                 '_TO', '_TO_PER_MATCH', 'BS', 'BS_PER_MATCH', 'ST', 'ST_PER_MATCH', 'TIME_PLAYED_IN_SEC', 'TOTAL_MATCHES']
      for team in team_list:
        players_stats = self.get_players_total_per_match_by_team(team[0])
        self.csv_handler.open_file(f"csv/teams/{team[0]}_stats.csv")
        self.csv_handler.write_row_headers(headers)
        self.csv_handler.write_row_values(players_stats)
        self.csv_handler.close_file()
        
    def create_player_stat_table(self):
      if self.db_manager.check_table_exists("PLAYER_STATS"):
        self.db_manager.delete_player_stat_table()
      self.db_manager.create_player_stat_table()
      
    def create_team_stat_table(self):
      if self.db_manager.check_table_exists("TEAM_STATS"):
        self.db_manager.delete_team_stat_table()
      self.db_manager.create_team_stat_table()
      
      
    def run(self):
      print("Fetching for files...")
      pdf_files = self.get_pdf_files()
      print(f"{len(pdf_files)} files found!")
      for index, pdf in enumerate(pdf_files):
          if self.db_manager.check_file_uploaded(pdf):
            print(f"File: {pdf}\n was already scanned!")
            continue
          print(f"\rExtracting data {index+1} / {len(pdf_files)}", end="")
          self.extract_match_data(pdf)
      print("\nDone!")
      
      print("Creating PLAYER_STATS and TEAM_STATS tables...")
      
      players_data = self.get_players_total_per_match()
      
      team_data = self.get_teams_total_per_match()
      
      self.create_player_stat_table()
      self.create_team_stat_table()
      
      for player in players_data:
        self.db_manager.insert_player_stats(player)
        
      for team in team_data:
        self.db_manager.insert_team_stats(team)
        
      self.db_manager.connection.commit()
      
      print("Done!")
      
      print("Creating CSV Files...")
      
      if not os.path.isdir("csv/teams"):
        os.makedirs("csv/teams")
      
      if not os.path.isdir("csv/players"):
        os.makedirs("csv/players")

      self.get_teams_stats()
      
      self.get_promediodetres()
      self.get_teampromediodetres()
      
      self.get_teamprompuntos()
      self.get_prompuntos()

      self.get_rebotes()
      self.get_teamrebotes()
      
      self.get_anotaciones()
      self.get_teamanotaciones()
      
      self.get_tiroslibres()
      self.get_teamtiroslibres()
      
      self.get_asistencias()
      self.get_teamasistencias()
      
      self.get_steals()
      self.get_teamsteals()
      
      self.get_bloqueos()
      self.get_teambloqueos()
      
      self.db_manager.connection.close()
      
      print("Done!")

    # workflow of extraction and insertion of the data into the database
    def run_all(self):
        print("Fetching for files...")
        pdf_files = self.get_pdf_files()
        print(f"{len(pdf_files)} files found!")
        for index, pdf in enumerate(pdf_files):
            if self.db_manager.check_file_uploaded(pdf):
              print(f"File: {pdf}\n was already scanned!")
              continue
            print(f"\rExtracting data {index+1} / {len(pdf_files)}", end="")
            self.extract_match_data(pdf)
        self.db_manager.connection.commit()
        self.db_manager.connection.close()
        print("\nDone!")
        
    def run_pdfs(self, pdf_list):
        print(f"{len(pdf_list)} files found!")
        for index, pdf in enumerate(pdf_list):
            print(f"\rExtracting data {index+1} / {len(pdf_list)}", end="")
            self.extract_match_data(pdf)
        self.db_manager.connection.commit()
        self.db_manager.connection.close()
        print("\nDone!")
