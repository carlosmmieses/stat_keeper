from os import stat
from stat_keeper.stat_keeper import StatKeeper
from sys import argv

if __name__ == "__main__":
    # We check for the arguments to see how to proceed
    init_stat_keeper = StatKeeper()
    init_stat_keeper.setup()
    # We start the workflow to read all PDFs in the PDF folder and 
    # add the info to the database in case the file was not scanned already
    stat_keeper = StatKeeper()

    if any([(lambda x: x in argv)(x) for x in ["-a", "--all"]]):
      stat_keeper.run_all()
        
    if any([(lambda x: x in argv)(x) for x in ["-at", "--all_teams"]]):
      stat_keeper.get_all_teams()
      
    if any([(lambda x: x in argv)(x) for x in ["-ap", "--all_players"]]):
      stat_keeper.get_all_players()
      
    if any([(lambda x: x in argv)(x) for x in ["-p", "--player_total"]]):
      stat_keeper.get_player_total(argv[2], argv[3])
      
    if any([(lambda x: x in argv)(x) for x in ["-pt", "--players_total"]]):
      stat_keeper.get_players_total()
      
    if any([(lambda x: x in argv)(x) for x in ["-ptm", "--players_total_per_match"]]):
      stat_keeper.export_player_data_per_match_to_csv()
      
    if any([(lambda x: x in argv)(x) for x in ["-ptf", "--players_total_file"]]):
      stat_keeper.export_player_data_to_csv()
      
    if any([(lambda x: x in argv)(x) for x in ["-t", "--team_total"]]):
      stat_keeper.get_team_total(argv[2])
      
    if any([(lambda x: x in argv)(x) for x in ["-tt", "--teams_total"]]):
      stat_keeper.get_teams_total()
      
    if any([(lambda x: x in argv)(x) for x in ["-ttf", "--teams_total_file"]]):
      stat_keeper.export_team_data_to_csv()
      
    if any([(lambda x: x in argv)(x) for x in ["-ttm", "--teams_total_per_match"]]):
      stat_keeper.export_team_data_per_match_to_csv()
      
    if any([(lambda x: x in argv)(x) for x in ["-ttr", "--team_to_rival"]]):
      stat_keeper.export_team_data_rival_to_csv(argv[2], argv[3])
      
    else:
      stat_keeper.run()