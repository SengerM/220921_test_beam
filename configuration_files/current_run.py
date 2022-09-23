from pathlib import Path
from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat

Alberto = RunBureaucrat(Path.home()/Path('measurements_data/20220923_2TIs_2MSs'))
Alberto.create_run()
