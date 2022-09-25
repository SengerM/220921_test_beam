from pathlib import Path
from the_bureaucrat.bureaucrats import RunBureaucrat # https://github.com/SengerM/the_bureaucrat

Alberto = RunBureaucrat(Path.home()/Path('measurements_data/20220925_3TIs_1PIN'))
Alberto.create_run()
